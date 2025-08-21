
import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

from logger import setup_logging
from tqdm import tqdm
from rapidfuzz import process, fuzz
from utils import normalize_title, chunked, write_batch_to_parquet, merge_and_cleanup_batches, upload_parquet_and_remove_local, get_most_recent_file
import polars as pl
import numpy as np

logger = setup_logging()

def apply_limit_to_matches(matches_by_payroll_index, payroll_data, lightcast_data, limit_per_job, output_buffer, lightcast_title_field, lightcast_keep_cols):
	for payroll_global_index, match_list in matches_by_payroll_index.items():
		sorted_matches = sorted(match_list, key=lambda pair: pair[1], reverse=True)
		if limit_per_job is not None:
			sorted_matches = sorted_matches[:limit_per_job]

		payroll_row = payroll_data[payroll_global_index]
		for lightcast_index, score in sorted_matches:
			lightcast_row = lightcast_data[lightcast_index]
			out_row = {**payroll_row}
			out_row["lightcast_matched_occupation"] = lightcast_row.get(lightcast_title_field)
			out_row["lightcast_match_score"] = score
			for col in lightcast_keep_cols:
				out_row[col] = lightcast_row.get(col)
			output_buffer.append(out_row)


def fuzzy_match_jobs_to_lightcast_vectorized(
	payroll_jobs_path,
	lightcast_path,
	output_parquet,
	score_cutoff,
	token_set_threshold,
	limit_per_job,
	payroll_chunk_size,
	batch_size
):
	try:
		payroll_file = get_most_recent_file(payroll_jobs_path)
	except FileNotFoundError:
		raise FileNotFoundError(f"No payroll parquet file found for: {payroll_jobs_path}")

	try:
		lightcast_file = get_most_recent_file(lightcast_path)
	except FileNotFoundError:
		raise FileNotFoundError(f"No lightcast parquet file found for: {lightcast_path}")

	payroll_df = pl.read_parquet(payroll_file)
	lightcast_df = pl.read_parquet(lightcast_file)

	title_field_candidates = ["business_title", "job_title", "title_description"]
	payroll_title_field = next((candidate for candidate in title_field_candidates if candidate in payroll_df.columns), None)
	if payroll_title_field is None:
		raise ValueError(f"Could not find a title column in payroll file. Searched: {title_field_candidates}")

	lc_candidates = ["Occupation (SOC)", "occupation", "occupation_name", "Occupation"]
	lightcast_title_field = next((candidate for candidate in lc_candidates if candidate in lightcast_df.columns), None)
	if lightcast_title_field is None:
		raise ValueError(f"Could not find an occupation column in lightcast file. Searched: {lc_candidates}")

	payroll_data = payroll_df.to_dicts()
	lightcast_data = lightcast_df.to_dicts()

	payroll_titles_norm = [normalize_title(row.get(payroll_title_field, "")) for row in payroll_data]
	lightcast_titles_norm = [normalize_title(row.get(lightcast_title_field, "")) for row in lightcast_data]

	# Create lookup from normalized lightcast title -> original row index (if duplicates, keep first)
	lightcast_lookup = {}
	for index, raw in enumerate(lightcast_data):
		key = lightcast_titles_norm[index]
		if key not in lightcast_lookup:
			lightcast_lookup[key] = index

	lightcast_keep_cols= []
	for candidate_col in ["Total Postings (Jan 2024 - Jun 2025)", "Median Posting Duration", "Total Postings", "Median Posting Duration (days)"]:
		if candidate_col in lightcast_df.columns and candidate_col not in lightcast_keep_cols:
			lightcast_keep_cols.append(candidate_col)

	output_buffer = []
	batch_count = 0

	total_chunks = (len(payroll_titles_norm) + payroll_chunk_size - 1) // payroll_chunk_size
	for start_index, end_index, payroll_chunk in tqdm(
		chunked(payroll_titles_norm, payroll_chunk_size),
		total=total_chunks,
		desc="Matching jobs -> lightcast (vectorized, chunked)"
	):
		# token_set prefilter between lightcast titles (rows) and payroll chunk (cols)
		similarity_matrix_token = process.cdist(
			lightcast_titles_norm,
			payroll_chunk,
			scorer=fuzz.token_set_ratio,
			score_cutoff=token_set_threshold,
			workers=-1,
			dtype=np.uint8,
		)

		lightcast_indices, chunk_payroll_indices = np.nonzero(similarity_matrix_token)
		if lightcast_indices.size == 0:
			continue

		matches_by_payroll_index = {}
		for lightcast_index, payroll_local_index in zip(lightcast_indices, chunk_payroll_indices):
			payroll_global_index = start_index + int(payroll_local_index)

			match_score = fuzz.WRatio(lightcast_titles_norm[lightcast_index], payroll_titles_norm[payroll_global_index])
			if match_score >= score_cutoff:
				# Optionally enforce a per-job limit (top N lightcast matches)
				matches_by_payroll_index.setdefault(payroll_global_index, []).append((lightcast_index, int(match_score)))

		# Apply limit_per_job and append to output_buffer via helper
		apply_limit_to_matches(matches_by_payroll_index, payroll_data, lightcast_data, limit_per_job, output_buffer, lightcast_title_field, lightcast_keep_cols)

		if len(output_buffer) >= batch_size:
			batch_count = write_batch_to_parquet(output_buffer, None, output_parquet, batch_count)

	# flush last batch
	if output_buffer:
		batch_count = write_batch_to_parquet(output_buffer, None, output_parquet, batch_count)

	logger.info(f"Intermediate matching complete. {batch_count} batch files written.")

	merge_and_cleanup_batches(output_parquet, logger)
	upload_parquet_and_remove_local(output_parquet, logger)

	logger.info(
		"Notes:\n"
		f" - Compared {len(lightcast_titles_norm):,} Lightcast occupations against {len(payroll_titles_norm):,} payroll/job titles.\n"
		f" - Score cutoff (WRatio): {score_cutoff}\n"
		f" - Token set threshold: {token_set_threshold}\n"
		f" - Limit per payroll title: {limit_per_job}\n"
		f" - Payroll chunk size: {payroll_chunk_size}\n"
		f" - Written in batches of {batch_size} rows."
	)


if __name__ == "__main__":
	fuzzy_match_jobs_to_lightcast_vectorized(
		payroll_jobs_path="data/BRONZE/payroll_to_jobs_title_fuzzy_matches/",
		lightcast_path="data/BRONZE/lightcast_top_posted_occupations_SOC/",
		output_parquet="data/BRONZE/jobs_to_lightcast_title_fuzzy_matches.parquet",
		score_cutoff=75,
		token_set_threshold=75,
		limit_per_job=None,
		payroll_chunk_size=100_000,
		batch_size=100_000,
	)