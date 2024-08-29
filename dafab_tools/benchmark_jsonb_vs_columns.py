# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import re
import subprocess
import time

from psycopg2 import connect, sql

# Database connection parameters
connection_params = {
    "host": "192.168.1.103",
    "port": "5432",
    "dbname": "rucio",
    "user": "rucio",
    "password": "secret"
}

# For stats
"""
SHOW max_parallel_workers_per_gather;
SHOW max_parallel_workers;
SHOW min_parallel_table_scan_size;
SHOW min_parallel_index_scan_size;
"""


# Fixed columns
evaluated_fixed_column_queries = [
    # """
    # SELECT name
    # FROM dev.did_meta
    # WHERE
    #     fix_var2 = 'SENTINEL-2';
    # """,
    #
    # """
    # SELECT name
    # FROM dev.did_meta
    # WHERE
    #     fix_var1 > 60.0
    #     OR
    #     fix_var2 = 'SENTINEL-2';
    # """,

    """
    SELECT name
    FROM dev.did_meta
    WHERE
        (
            fix_var1 > 60.0
            AND
            fix_var2 = 'SENTINEL-2'
        )
        AND
        (
            created_at >= '2023-01-01 01:00:00'::timestamp
            AND
            created_at <= '2024-01-01 01:00:00'::timestamp
        );
    """,

    # """
    # SELECT name
    # FROM dev.did_meta
    # WHERE
    #     (
    #         fix_var1 > 60.0
    #         OR
    #         fix_var2 = 'SENTINEL-2'
    #     )
    #     AND
    #     (
    #         created_at >= '2023-01-01 01:00:00'::timestamp
    #         AND
    #         created_at <= '2024-01-01 01:00:00'::timestamp
    #     );
    # """
]

# JSONB columns
evaluated_json_queries = [
    # """
    # SELECT name
    # FROM dev.did_meta
    # WHERE
    #     structured_meta->>'collection' = 'SENTINEL-2';
    # """,
    #
    # """
    # SELECT name
    # FROM dev.did_meta
    # WHERE
    #     (structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float > 60.0
    #     OR
    #     structured_meta->>'collection' = 'SENTINEL-2';
    # """,

    """
    SELECT name
    FROM dev.did_meta
    WHERE
        (
            (structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float > 60.0
            AND
            structured_meta->>'collection' = 'SENTINEL-2'
        )
        AND
        (
            structured_meta->'properties'->>'datetime' >= '2023-01-01T01:00:00Z'
            AND
            structured_meta->'properties'->>'datetime' <= '2024-01-01T01:00:00Z'
        );
    """,

    # """
    # SELECT name
    # FROM dev.did_meta
    # WHERE
    #     (
    #         (structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float > 60.0
    #         OR
    #         structured_meta->>'collection' = 'SENTINEL-2'
    #     )
    #     AND
    #     (
    #         structured_meta->'properties'->>'datetime' >= '2023-01-01T01:00:00Z'
    #         AND
    #         structured_meta->'properties'->>'datetime' <= '2024-01-01T01:00:00Z'
    #     );
    # """
]

# For index creation
"""
-- A B-tree index on fix_var2
CREATE INDEX idx_fix_var2
ON dev.did_meta (
    fix_var2
);
-- A GIN index on collection
CREATE INDEX idx_structured_meta_collection
ON dev.did_meta USING GIN (
    structured_meta->>'collection'
);
-- A B-tree index on collection
CREATE INDEX idx_did_meta_structured_meta_collection
ON dev.did_meta (
    (structured_meta->>'collection')
);


-- A B-tree index on fix_var1
CREATE INDEX idx_fix_var1
ON dev.did_meta (
    fix_var1
);
-- A GIN index on the cast float value of surface_of_observed_water_bodies field
CREATE INDEX idx_surface_of_observed_water_bodies
ON dev.did_meta USING GIN (
    ((structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float)
);
-- A B-tree index on the cast float value of surface_of_observed_water_bodies field
CREATE INDEX idx_flood_detection_float
ON dev.did_meta (
    ((structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float)
);


-- A B-tree index on created_at
CREATE INDEX idx_created_at
ON dev.did_meta (
    created_at
);
-- A GIN index on the cast timestamp value of datetime field
CREATE INDEX idx_structured_meta_datetime
ON dev.did_meta USING GIN (
    ((structured_meta->'properties'->>'datetime')::timestamp)
);
-- A B-tree index on the cast timestamp value of datetime field
CREATE INDEX idx_did_meta_structured_meta_datetime
ON dev.did_meta (
    ((structured_meta->'properties'->>'datetime')::timestamp)
);


-- Combinations
CREATE INDEX idx_fix_var1_var2 ON dev.did_meta (fix_var1, fix_var2);
CREATE INDEX idx_fix_var1_var2_created_at ON dev.did_meta (fix_var1, fix_var2, created_at);
CREATE INDEX idx_flood_detection_surface_collection ON dev.did_meta (
    (structured_meta->>'collection'),
    ((structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float)
);
CREATE INDEX idx_collection_flood_detection_surface ON dev.did_meta (
    ((structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float),
    (structured_meta->>'collection')
);
CREATE INDEX idx_flood_datetime ON dev.did_meta (
    ((structured_meta->>'collection')),
    ((structured_meta->'properties'->>'datetime')),
    ((structured_meta->'properties'->'dfe:flood_detection'->>'surface_of_observed_water_bodies')::float)
);
"""


# For restarting the database
"""
docker-compose --file etc/docker/dev/docker-compose.yml pull ruciodb && docker-compose --file etc/docker/dev/docker-compose.yml up -d --force-recreate ruciodb
docker exec dev-ruciodb-1 psql -U rucio -d rucio -c "SELECT pg_postmaster_start_time();"
"""


def get_did_meta_info(table):

    global connection_params

    try:
        # Connect to the PostgreSQL database
        conn = connect(**connection_params)
        cur = conn.cursor()

        # Query to count entries in dev.{table}
        count_query = sql.SQL(f"SELECT COUNT(*) FROM dev.{table};")
        cur.execute(count_query)
        count = cur.fetchone()[0]

        # Query to get the size of the {table} data in MB (without indexes)
        data_size_query = sql.SQL(
            f"SELECT pg_size_pretty(pg_table_size('dev.{table}'));"
        )
        cur.execute(data_size_query)
        data_size = cur.fetchone()[0]

        # Query to get the size of the indexes on the {table} in MB
        index_size_query = sql.SQL(
            f"SELECT pg_size_pretty(pg_indexes_size('dev.{table}'));"
        )
        cur.execute(index_size_query)
        index_size = cur.fetchone()[0]

        # Query to get the total size of the table, including indexes (already included in your earlier script)
        total_size_query = sql.SQL(
            f"SELECT pg_size_pretty(pg_total_relation_size('dev.{table}'));"
        )
        cur.execute(total_size_query)
        total_size = cur.fetchone()[0]

        # Close the cursor and connection
        cur.close()
        conn.close()

        return count, data_size, index_size, total_size

    except Exception as e:
        print(f"Error: {e}")
        return -1, None, None, None


def execute_evaluation_query(query, num_runs=6):
    """
    Executes an EXPLAIN ANALYZE query n times to evaluate the average performance of the provided SQL query.
    Returns the execution plan and performance details.
    """
    results = []

    first_execution = True
    try:
        for _ in range(num_runs):

            # Run Docker commands at the start
            if not reset_optimizations():
                print("Exiting due to errors with Docker setup.")
                exit(1)

            # Connect to the PostgreSQL database
            conn = connect(**connection_params)
            cur = conn.cursor()

            # Execute the EXPLAIN ANALYZE query
            explain_query = sql.SQL(
                "SET jit = off; "  # Disable JIT optimizations
                "SET enable_bitmapscan = OFF; "  # Disable bitmap index scans
                "SET max_parallel_workers_per_gather = 1; "  # Force the use of 1 worker
                "SET enable_seqscan = OFF; "  # Option: Enable sequential scan
                "SET enable_indexscan = ON; "  # Option: Disable normal index scans
                "EXPLAIN ANALYZE {}"
            ).format(sql.SQL(query))
            cur.execute(explain_query)

            # Fetch and return all the rows from the EXPLAIN ANALYZE output
            explain_results = cur.fetchall()

            if not first_execution:
                results.append(parse_explain_analyze(explain_results))
            else:
                first_execution = False

            # Close the cursor and connection
            cur.close()
            conn.close()

        print(results)

        # Calculate average metrics
        avg_results = {key: sum(result[key] for result in results) / (num_runs-1)
                       for key in results[0] if isinstance(results[0][key], (int, float))}

        # print(avg_results)

        return avg_results
    except Exception as e:
        print(f"Error executing evaluation query: {e}")
        return None


def parse_explain_analyze(results):

    print(results)

    """
    Parses the EXPLAIN ANALYZE output and extracts key performance metrics.
    Returns a dictionary with extracted values.
    """
    metrics = {
        'execution_time': None,
        'planning_time': None,
        'workers_planned': 1,
        'workers_launched': 1,
        'seq_scan': 0,
        'index_scan': 0,
        'rows_returned': None
    }

    gather_complete = False

    for row in results:
        line = row[0]

        # Extract Execution Time
        if "Execution Time" in line:
            match = re.search(r"Execution Time: ([\d.]+) ms", line)
            if match:
                metrics['execution_time'] = float(match.group(1))

        # Extract Planning Time
        if "Planning Time" in line:
            match = re.search(r"Planning Time: ([\d.]+) ms", line)
            if match:
                metrics['planning_time'] = float(match.group(1))

        # Extract Workers Planned and Launched
        if "Workers Planned" in line:
            match = re.search(r"Workers Planned: (\d+)", line)
            if match:
                metrics['workers_planned'] = int(match.group(1))

        if "Workers Launched" in line:
            match = re.search(r"Workers Launched: (\d+)", line)
            if match:
                metrics['workers_launched'] = int(match.group(1))

        # Check for Sequential Scan
        if "Seq Scan" in line:
            metrics['seq_scan'] = 1

        # Check for Scan on did_meta to extract the total returned rows
        if ("Scan on did_meta" in line or "Index Scan using" in line) and gather_complete is False:
            match = re.search(r"actual time=[\d.]+..[\d.]+ rows=(\d+) loops=", line)
            if match:
                metrics['rows_returned'] = int(match.group(1))

        # Check for Index Scan
        if "Index Scan" in line:
            metrics['index_scan'] = 1

        # Extract Rows Returned
        if ("Gather" in line) or (("Seq Scan" in line) and ("Parallel" not in line)):
            match = re.search(r"actual time=[\d.]+..[\d.]+ rows=(\d+) loops=", line)
            if match:
                metrics['rows_returned'] = int(match.group(1))
                gather_complete = True

    return metrics


def reset_optimizations():
    # Define the absolute path to your docker-compose.yml file
    compose_file_path = "/home/gouz/rucio/etc/docker/dev/docker-compose.yml"

    # Pull and recreate the ruciodb container
    docker_compose_cmd = f"docker-compose --file {compose_file_path} pull ruciodb && docker-compose --file {compose_file_path} up -d --force-recreate ruciodb"
    try:
        subprocess.run(docker_compose_cmd, shell=True, check=True)
        # print("Docker ruciodb pulled and recreated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error while pulling and recreating ruciodb: {e}")
        return False

    time.sleep(5)

    # Execute the psql command inside the ruciodb container
    docker_exec_cmd = "docker exec dev-ruciodb-1 psql -U rucio -d rucio -c 'SELECT pg_postmaster_start_time();'"
    try:
        result = subprocess.run(docker_exec_cmd, shell=True, check=True, capture_output=True, text=True)
        # print(f"Postmaster start time:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error while running psql command: {e}")
        return False


if __name__ == "__main__":

    # Run Docker commands at the start
    if not reset_optimizations():
        print("Exiting due to errors with Docker setup.")
        exit(1)

    for _table in ["did_meta", "dids"]:
        _count, _data_size, _index_size, _total_size = get_did_meta_info(_table)
        # print(f"The count of entries in dev.{_table} table is: {_count}")
        # print(f"The size of the {_table} table (without indexes) is: {_data_size}")
        # print(f"The size of the indexes on the {_table} table is: {_index_size}")
        # print(f"The total size of the {_table} table (including indexes) is: {_total_size}")
        print(f"{_table};{_count};{_data_size};{_index_size};{_total_size}")

    print(" ")

    for query_id, evaluation_query in enumerate(evaluated_fixed_column_queries):
        evaluated_query_results = execute_evaluation_query(evaluation_query)
        print(f"{query_id};"
              f"{int(evaluated_query_results['execution_time'])};"
              f"{evaluated_query_results['planning_time']};"
              f"{evaluated_query_results['workers_planned']};"
              f"{evaluated_query_results['workers_launched']};"
              f"{evaluated_query_results['seq_scan']};"
              f"{evaluated_query_results['index_scan']};"
              f"{int(evaluated_query_results['rows_returned'])}")
        # print(evaluated_query_results)

    print(" ")

    for query_id, evaluation_query in enumerate(evaluated_json_queries):
        evaluated_query_results = execute_evaluation_query(evaluation_query)
        print(f"{query_id};"
              f"{int(evaluated_query_results['execution_time'])};"
              f"{evaluated_query_results['planning_time']};"
              f"{evaluated_query_results['workers_planned']};"
              f"{evaluated_query_results['workers_launched']};"
              f"{evaluated_query_results['seq_scan']};"
              f"{evaluated_query_results['index_scan']};"
              f"{int(evaluated_query_results['rows_returned'])}")

        # print(evaluated_query_results)
