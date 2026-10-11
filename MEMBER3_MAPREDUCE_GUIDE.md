# Member 3 MapReduce Guide

## 1. Project Overview

This project converts sales data into a top-selling product ranking using a Kafka-to-HDFS-to-Hadoop pipeline.

The intended flow is:

```text
Sales CSV Dataset
       |
       v
Kafka Producer
       |
       v
Kafka Topic: sales-transactions
       |
       v
Kafka Consumer
       |
       v
HDFS: /sales/input
       |
       v
Hadoop Streaming Mapper
       |
       v
Hadoop Streaming Reducer
       |
       v
HDFS: /sales/output/top-products
       |
       v
Top 10 Products CSV
```

Member 3 is responsible for the MapReduce implementation and validation:

- `mapreduce/mapper.py`
- `mapreduce/reducer.py`
- parsing transaction JSON
- aggregating units sold per product
- sorting by total quantity descending
- alphabetical tie-breaking
- selecting the top 10 products
- checking invalid input
- ensuring compatibility with Hadoop Streaming

Members 1, 2, and 4 handle the remaining pipeline work (producer, consumer, Docker, scheduler, and orchestration).

## 2. Member 3 Responsibilities

Member 3 owns the ranking logic only.

Responsibilities:

- read JSON transaction records
- extract `product_name` and `units_sold`
- validate records
- produce mapper output as `product_name\tunits_sold`
- aggregate totals in the reducer
- rank by largest quantity first
- break ties alphabetically
- keep at most 10 products
- output CSV with `rank,product_name,units_sold`
- validate edge cases and error handling
- test the actual scripts with Python `unittest`

## 3. Existing Implementation Before My Work

Before this session, the repository already contained the following:

- `[mapreduce/mapper.py](mapreduce/mapper.py)`
- `[mapreduce/reducer.py](mapreduce/reducer.py)`

The mapper already:

- reads JSON Lines from standard input
- parses `product_name` and `units_sold`
- validates malformed or invalid records
- rejects empty product names, negative quantities, and invalid values
- writes tab-separated mapper output

The reducer already:

- reads mapper output lines from stdin
- adds quantities per product
- sorts descending by quantity
- breaks ties alphabetically
- keeps the top 10 results
- writes CSV output with a header row

This code was retained because it already matches the project requirements and did not require a broad rewrite.

## 4. Work Implemented or Improved

I created the automated validation test suite:

- `tests/test_mapreduce.py`

This test suite executes the real mapper and reducer scripts using subprocesses and validates normal, edge-case, and failure scenarios.

No production code files were modified under Member 3 because the existing implementation already matches the required behavior.

### Files created or modified

- Created: `tests/test_mapreduce.py`

## 5. Mapper Explanation

The mapper reads JSON Lines from standard input and converts them into product/quantity pairs.

Example JSON input:

```json
{"product_name":"Laptop","units_sold":5}
{"product_name":"Mouse","units_sold":10}
{"product_name":"Keyboard","units_sold":7}
{"product_name":"Laptop","units_sold":8}
```

Mapper output:

```text
Laptop	5
Mouse	10
Keyboard	7
Laptop	8
```

Validation logic includes:

- JSON must be valid
- required keys must exist
- `product_name` cannot be empty
- `units_sold` must be convertible to `int`
- negative values are rejected
- product names containing tabs/newlines are rejected

## 6. Reducer Explanation

The reducer reads mapper output and aggregates totals per product.

Example reducer input:

```text
Laptop	5
Mouse	10
Keyboard	7
Laptop	8
Mouse	3
Monitor	6
Headphones	15
Keyboard	6
```

Aggregation result:

```text
Laptop: 13
Mouse: 13
Keyboard: 13
Monitor: 6
Headphones: 15
```

Sorting rule:

1. higher quantity first
2. alphabetical order for equal totals

Result:

```csv
rank,product_name,units_sold
1,Headphones,15
2,Keyboard,13
3,Laptop,13
4,Mouse,13
5,Monitor,6
```

## 7. Complete Worked Example

Sample input:

```json
{"product_name":"Laptop","units_sold":5}
{"product_name":"Mouse","units_sold":10}
{"product_name":"Keyboard","units_sold":7}
{"product_name":"Laptop","units_sold":8}
{"product_name":"Mouse","units_sold":3}
{"product_name":"Monitor","units_sold":6}
{"product_name":"Headphones","units_sold":15}
{"product_name":"Keyboard","units_sold":6}
```

Mapper output:

```text
Laptop	5
Mouse	10
Keyboard	7
Laptop	8
Mouse	3
Monitor	6
Headphones	15
Keyboard	6
```

Aggregated totals:

```text
Headphones: 15
Keyboard: 13
Laptop: 13
Mouse: 13
Monitor: 6
```

Final CSV:

```csv
rank,product_name,units_sold
1,Headphones,15
2,Keyboard,13
3,Laptop,13
4,Mouse,13
5,Monitor,6
```

## 8. Requirements and Prerequisites

Verified requirements for local Python validation:

- Python 3.14.7
- Windows PowerShell
- VS Code repository at `D:\top-selling-product-ranking`
- Git branch: `feature/mapreduce-ranking`

For Docker/Hadoop integration:

- Docker Desktop
- Docker Compose
- Kafka and Hadoop services from the project compose file
- the Hadoop Streaming JAR in the pipeline container

## 9. How to Test the Mapper

PowerShell commands:

```powershell
Set-Location "D:\top-selling-product-ranking"

@'
{"product_name":"Laptop","units_sold":5}
{"product_name":"Mouse","units_sold":10}
{"product_name":"Keyboard","units_sold":7}
{"product_name":"Laptop","units_sold":8}
'@ | python .\mapreduce\mapper.py
```

Expected output:

```text
Laptop	5
Mouse	10
Keyboard	7
Laptop	8
```

Pass/fail criteria:

- exit code 0
- stdout matches the expected records
- stderr is empty

## 10. How to Test the Reducer

PowerShell commands:

```powershell
Set-Location "D:\top-selling-product-ranking"

@'
Laptop	5
Mouse	10
Keyboard	7
Laptop	8
Mouse	3
Monitor	6
Headphones	15
Keyboard	6
'@ | python .\mapreduce\reducer.py
```

Expected output:

```csv
rank,product_name,units_sold
1,Headphones,15
2,Keyboard,13
3,Laptop,13
4,Mouse,13
5,Monitor,6
```

Pass/fail criteria:

- exit code 0
- CSV header is correct
- rank numbers start at 1
- totals and sort order match expectations

## 11. How to Test Mapper and Reducer Together

PowerShell commands:

```powershell
Set-Location "D:\top-selling-product-ranking"

@'
{"product_name":"Laptop","units_sold":5}
{"product_name":"Mouse","units_sold":10}
{"product_name":"Keyboard","units_sold":7}
{"product_name":"Laptop","units_sold":8}
{"product_name":"Mouse","units_sold":3}
{"product_name":"Monitor","units_sold":6}
{"product_name":"Headphones","units_sold":15}
{"product_name":"Keyboard","units_sold":6}
'@ | python .\mapreduce\mapper.py | python .\mapreduce\reducer.py
```

Expected output:

```csv
rank,product_name,units_sold
1,Headphones,15
2,Keyboard,13
3,Laptop,13
4,Mouse,13
5,Monitor,6
```

## 12. Automated Unit Tests

The automated tests live in:

- `tests/test_mapreduce.py`

They use Python `unittest` and execute the actual scripts with `subprocess.run`.

Command:

```powershell
Set-Location "D:\top-selling-product-ranking"
python -m unittest discover -s tests -p "test_mapreduce.py" -v
```

Actual result that was executed in this session:

```text
Ran 14 tests in 0.647s

OK

EXIT=0
```

This confirms the current local Member 3 logic passes the implemented test suite.

## 13. Expected Output and Ranking Verification

Expected output for the sample data:

```csv
rank,product_name,units_sold
1,Headphones,15
2,Keyboard,13
3,Laptop,13
4,Mouse,13
5,Monitor,6
```

Checks:

- top 10 count is at most 10
- totals are correct
- quantity order is descending
- alphabetical order is used for ties
- ranks start from 1
- CSV header is correct

## 14. Error Handling and Edge Cases

The mapper rejects:

- malformed JSON
- missing required fields
- negative values
- invalid numeric types
- empty names
- product names with tabs or newlines

The reducer rejects malformed mapper input such as lines without a tab separator.

Current behavior is strict and intentionally fails fast for invalid data.

## 15. Hadoop Streaming Integration

The scheduler uses:

```bash
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.framework.name=local \
  -D mapreduce.job.reduces=1 \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH" \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file /app/mapreduce/mapper.py \
  -file /app/mapreduce/reducer.py
```

This means:

- one reducer is configured
- the reducer is expected to produce the global ranking for the entire job
- HDFS input sits under `/sales/input`
- HDFS output is under `/sales/output/top-products`
- the mapper and reducer run as Python scripts via Hadoop Streaming

This matches the current local logic and is compatible with the existing scheduler contract.

## 16. Integration With Other Members

Member 3 integrates with the rest of the pipeline as follows:

- Member 2 consumer writes JSON records with `product_name` and `units_sold`
- Member 4 scheduler invokes Hadoop Streaming using the mapper and reducer
- HDFS stores records under `/sales/input`
- the Mapper/Reducer pipeline produces the top 10 CSV
- Docker starts Kafka, HDFS, and the pipeline service

## 17. GitHub Branch and Pull Request Workflow

To verify the current branch:

```powershell
git -C "D:\top-selling-product-ranking" branch --show-current
```

To review the working tree:

```powershell
git -C "D:\top-selling-product-ranking" status
```

To stage only the Member 3 files:

```powershell
git -C "D:\top-selling-product-ranking" add tests\test_mapreduce.py MEMBER3_MAPREDUCE_GUIDE.md
```

To commit:

```powershell
git -C "D:\top-selling-product-ranking" commit -m "Add MapReduce validation tests"
```

To push:

```powershell
git -C "D:\top-selling-product-ranking" push origin feature/mapreduce-ranking
```

Then create a pull request from `feature/mapreduce-ranking` into `main`.

## 18. Testing After Merging

After the branch is merged, the team can reproduce local tests with:

```powershell
Set-Location "D:\top-selling-product-ranking"
python -m unittest discover -s tests -p "test_mapreduce.py" -v
```

## 19. Troubleshooting

Common issues:

- bad JSON in mapper input -> invalid JSON error
- missing keys -> `KeyError` reported and script exits non-zero
- invalid number -> `TypeError`/`ValueError` reported
- malformed reducer input -> missing tab separator error
- empty input -> valid no-data behavior for both scripts
- Docker build issue -> the configured base image uses unavailable Debian Stretch repositories
- Hadoop Streaming issue -> Hadoop and Java are present, but the containers lack Python 3
- Kafka metadata issue -> producer validation timed out while resolving broker metadata

## 20. Presentation and Viva Questions

### What did I do as Member 3?
I validated and tested the MapReduce ranking implementation and wrote a dedicated automated test suite for real script execution.

### What is MapReduce?
It is a two-stage data processing model where a mapper prepares key/value data and a reducer aggregates it into a final result.

### What does the mapper do?
It parses each transaction, extracts product and quantity, and outputs `product_name\tunits_sold`.

### What does the reducer do?
It totals all quantities for each product and writes the ranked top products as CSV.

### How is aggregation performed?
It sums the same product name across all mapper output records.

### How is the top 10 calculated?
It sorts by total quantity descending and then by product name alphabetically, then returns only the first 10.

### How are ties handled?
Alphabetical ordering is used for products with equal totals.

### Why use one reducer?
The scheduler is configured for a single reducer, so the reducer performs the global ranking across all products in the job.

### How did I test the implementation?
Using `unittest` with subprocess execution against the actual mapper and reducer scripts.

### How does it integrate with Kafka and HDFS?
The consumer writes valid JSON records into HDFS, which are then read by the Hadoop streaming mapper and reducer.

### What did I personally contribute?
I verified the ranking logic, created real validation tests, and documented the expected behavior and usage.

## 21. Final Completion Checklist

The following items are verified as of this session:

- [x] Mapper reviewed
- [x] Reducer reviewed
- [x] Aggregation verified
- [x] Ranking verified
- [x] Tie-breaking verified
- [x] Automated tests passed locally
- [x] Consumer compatibility verified by contract review
- [x] Scheduler configuration reviewed
- [x] Hadoop container runtime inspected
- [ ] Isolated real Hadoop Streaming execution passed (NOT RUN: Python 3 is missing)
- [ ] Full Kafka-to-HDFS pipeline passed
- [x] Documentation created
- [x] Git branch verified
- [ ] Pull request completed

## 22. Remaining Work and Limitations

The following items remain blocked by real runtime issues discovered during validation:

- full `pipeline` service startup inside Docker
- isolated real Hadoop Streaming run: Hadoop containers lack Python 3 required by the actual mapper and reducer
- real Kafka -> HDFS -> MapReduce run through the project scheduler
- scheduler runtime verification under the actual Compose stack
- end-to-end validation of the final CSV output inside Docker

These runtime blockers were reproduced during testing. The Dockerfile and Kafka issues are Member 4 integration dependencies; the existing Hadoop service images also lack the Python 3 runtime required by Member 3's scripts. No Member 3 source change is indicated by this preflight.

## 23. Docker and Hadoop Test Instructions

Docker environment inspection was completed. Kafka, NameNode, and DataNode were started; the pipeline image build failed. No project volumes or Kafka/HDFS data were deleted or reset.

```powershell
docker --version
docker compose version
docker compose config --services
```

Actual output observed during environment verification:

```text
Docker version 29.6.2, build dfc4efb
Docker Compose version v5.3.1
kafka
namenode
datanode
pipeline
```

Later, the live service status was confirmed as:

```text
NAMES                                    STATUS                   PORTS
top-selling-product-ranking-datanode-1   Up 5 minutes (healthy)   9864/tcp
top-selling-product-ranking-kafka-1      Up 5 minutes (healthy)   0.0.0.0:9092->9092/tcp
top-selling-product-ranking-namenode-1   Up 5 minutes (healthy)   0.0.0.0:9870->9870/tcp
```

The services were started using the existing Compose configuration without any resets or destructive cleanup.

This startup command was attempted; the `pipeline` image build failed with the Debian Stretch 404 errors documented below:

```powershell
Set-Location "D:\top-selling-product-ranking"
docker compose up --build -d
```

Then:

```powershell
docker compose ps
docker compose logs --tail=100 pipeline
docker compose logs --tail=100 kafka
docker compose logs --tail=100 namenode
docker compose logs --tail=100 datanode
```

Stop services with:

```powershell
docker compose down
```

No HDFS or Kafka data deletion was performed during this session without explicit approval.

## 23A. Verified Integration Blockers

The following runtime blockers were observed during the approved Docker integration testing.

### 1) Pipeline image build fails because Debian Stretch repositories are unavailable

Command executed:

```powershell
docker compose -f "D:\top-selling-product-ranking\docker-compose.yml" up -d
```

Observed failure:

```text
E: Failed to fetch http://deb.debian.org/debian/dists/stretch/main/binary-amd64/Packages 404 Not Found
E: Failed to fetch http://deb.debian.org/debian/dists/stretch-updates/main/binary-amd64/Packages 404 Not Found
E: Failed to fetch http://security.debian.org/debian-security/dists/stretch/updates/main/binary-amd64/Packages 404 Not Found
```

This is a real environment issue in the `Dockerfile`: it still relies on old Debian Stretch package sources, which no longer exist. This prevents the `pipeline` service from building.

### 2) Producer cannot reach Kafka metadata in the active environment

The service stack itself is healthy, but the producer still fails when trying to publish to Kafka from the host environment:

```text
KafkaTimeoutError: Failed to update metadata after 60.0 secs.
```

This was reproduced while running the external producer against `localhost:9092` with the real Kafka container running. The broker is healthy, but the host-side producer configuration and runtime environment still fail to establish topic metadata in this current setup.

### 3) Existing Hadoop containers do not have Python 3

Read-only inspection inside both healthy Hadoop containers found Java 8, Hadoop 3.2.1, and `/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar`. Neither container has `python3`: `command -v python3` returned no executable and `python3 --version` returned `bash: python3: command not found`.

Hadoop and HDFS are available by absolute path under `/opt/hadoop-3.2.1/bin`, although they are not on the non-interactive shell's `PATH`. That does not satisfy the scheduler's `python3 mapper.py` / `python3 reducer.py` requirement.

The isolated Hadoop Streaming test was therefore assessed but **NOT RUN**. No HDFS test paths were created. Read-only checks returned:

```text
/opt/hadoop-3.2.1/bin/hdfs dfs -ls /sales
ls: `/sales': No such file or directory
exit=1

/opt/hadoop-3.2.1/bin/hdfs dfs -ls /sales/output
ls: `/sales/output': No such file or directory
exit=1
```

Compose reported creating the named Kafka and HDFS volumes on first startup, and the NameNode startup log recorded formatting its new name directory. No existing Kafka topics, HDFS paths, result files, or volumes were deleted or reset. The Dockerfile Stretch issue, Kafka metadata timeout, and Python-enabled Hadoop runtime are integration dependencies for Member 4/the integration owner. No teammate source or Docker configuration was changed.

## 23B. Isolated Hadoop Streaming Test: NOT RUN

The real mapper and reducer are Python 3 scripts. Although Hadoop, Java, and the Streaming JAR are installed in the healthy NameNode container, the test cannot run until Python 3 is available in the container that launches Hadoop Streaming. The expected ranking below was independently derived from all 20 rows and 10 products in `data/sales.csv`:

```csv
rank,product_name,units_sold
1,Wireless Headphones,1842
2,Smart Watch,1560
3,Laptop Stand,1304
4,USB-C Hub,1048
5,Mechanical Keyboard,986
6,Bluetooth Speaker,872
7,Webcam,764
8,Portable SSD,693
9,Wireless Mouse,612
10,Phone Charger,548
```

### Safe reproduction commands after Python 3 is available

These PowerShell commands use a GUID-specific HDFS root, refuse to reuse an existing path, and do not delete data. They do not touch `/sales/input`, `/sales/output/top-products`, Kafka topics, Docker volumes, or project results. The unique test output is retained for inspection.

```powershell
Set-Location "D:\top-selling-product-ranking"
$container = "top-selling-product-ranking-namenode-1"
$id = [guid]::NewGuid().ToString("N")
$localJson = Join-Path $env:TEMP "member3-$id.jsonl"
$hdfsRoot = "/sales/member3/$id"
$hdfsInput = "$hdfsRoot/input"
$hdfsOutput = "$hdfsRoot/output"
$stageDir = "/tmp/member3-$id"
$remoteJson = "$stageDir/sample.jsonl"

# Required preflight; stop until Python 3 is installed in this container.
docker exec $container bash -lc 'command -v python3 && python3 --version'
if ($LASTEXITCODE -ne 0) { throw "Python 3 is unavailable; Hadoop Streaming test not run." }

# Convert the known sample CSV into JSON Lines in the Windows temp directory.
[string[]]$jsonLines = Import-Csv .\data\sales.csv | ForEach-Object {
       [ordered]@{
              product_name = $_.product_name.Trim()
              units_sold = [int]$_.units_sold
       } | ConvertTo-Json -Compress
}
[System.IO.File]::WriteAllLines($localJson, $jsonLines, [System.Text.UTF8Encoding]::new($false))

# Refuse to reuse any HDFS test path.
docker exec $container bash -lc "if /opt/hadoop-3.2.1/bin/hdfs dfs -test -e '$hdfsRoot'; then exit 73; else exit 0; fi"
if ($LASTEXITCODE -ne 0) { throw "The unique HDFS root already exists; generate a new GUID." }

# Stage unique temporary files and create only the isolated input path.
docker exec $container bash -lc "mkdir -p '$stageDir'"
if ($LASTEXITCODE -ne 0) { throw "Could not create the isolated staging directory." }
docker cp $localJson "${container}:$remoteJson"
if ($LASTEXITCODE -ne 0) { throw "Could not copy the JSONL input." }
docker cp .\mapreduce\mapper.py "${container}:$stageDir/mapper.py"
if ($LASTEXITCODE -ne 0) { throw "Could not copy the Member 3 mapper." }
docker cp .\mapreduce\reducer.py "${container}:$stageDir/reducer.py"
if ($LASTEXITCODE -ne 0) { throw "Could not copy the Member 3 reducer." }
docker exec $container bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p '$hdfsInput' && /opt/hadoop-3.2.1/bin/hdfs dfs -put '$remoteJson' '$hdfsInput/sample.jsonl'"
if ($LASTEXITCODE -ne 0) { throw "Could not create the isolated HDFS input." }

# This GUID output is new, so no output cleanup is needed.
docker exec $container bash -lc "/opt/hadoop-3.2.1/bin/hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -D mapreduce.framework.name=local -D mapreduce.job.reduces=1 -input '$hdfsInput' -output '$hdfsOutput' -mapper 'python3 mapper.py' -reducer 'python3 reducer.py' -file '$stageDir/mapper.py' -file '$stageDir/reducer.py'"
if ($LASTEXITCODE -ne 0) { throw "Hadoop Streaming failed; preserve the unique paths and inspect logs." }

$actual = docker exec $container bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -cat '$hdfsOutput'/part-*"
if ($LASTEXITCODE -ne 0) { throw "Could not read the unique Hadoop output." }
$actualText = ($actual -join "`n").Trim()
$expectedText = @'
rank,product_name,units_sold
1,Wireless Headphones,1842
2,Smart Watch,1560
3,Laptop Stand,1304
4,USB-C Hub,1048
5,Mechanical Keyboard,986
6,Bluetooth Speaker,872
7,Webcam,764
8,Portable SSD,693
9,Wireless Mouse,612
10,Phone Charger,548
'@
if ($actualText -cne $expectedText.Trim()) {
       Write-Output "Actual output:`n$actualText"
       throw "Hadoop output does not match the expected ranking."
}
Write-Output "PASS: output matches expected ranking. Retained HDFS root: $hdfsRoot"
```

The Python 3 preflight must pass in the same container that launches the local Hadoop job. If Python 3 is supplied in another runtime, that runtime must also be configured to reach the existing NameNode and DataNode. Enabling this runtime is an integration-owner change, not a Member 3 code change.

## 24. Complete Kafka → HDFS → MapReduce Test Instructions

After the integration-owner fixes the Docker build, Kafka metadata connectivity, and Python runtime dependencies, follow these steps to validate the full pipeline without impacting persistent state:

1. start the Docker services
2. verify Kafka and Hadoop are healthy
3. confirm the topic exists
4. confirm HDFS input is present
5. run the producer/consumer flow against a new temporary input path
6. inspect HDFS stored JSON records
7. run Hadoop Streaming with the existing mapper and reducer
8. inspect the output under a temporary output directory
9. confirm final CSV values and ordering
10. only then review or copy the results into the project result directory if explicitly requested

Recommended safe pattern for temporary validation:

- use a unique, temporary HDFS path such as `/sales/member3/<timestamp>-input`
- use a corresponding unique output path such as `/sales/member3/<timestamp>-output`
- do not delete existing `/sales/input` or `/sales/output/top-products` unless explicitly approved
- do not reset Docker volumes or Kafka topics without another approval step

## 25. Actual Integration Test Results

This section reflects the actual state of the project after the executed validation.

| Test | Expected | Actual | Status |
|---|---|---|---|
| Local Python tests | All pass | 14 tests ran and passed | PASS |
| Docker Compose startup | All services start | `pipeline` build failed due to old Debian Stretch repositories | FAIL |
| Kafka health | Service healthy | Kafka container is healthy and listening on 9092 | PASS |
| HDFS health | NameNode/Datanode healthy | HDFS returned 200 OK on localhost:9870 and reported one live datanode | PASS |
| Hadoop runtime preflight | Java, Python 3, Hadoop Streaming available | Java 8, Hadoop 3.2.1, Streaming JAR present; `python3` absent in both Hadoop containers | FAIL (preflight) |
| Isolated Hadoop Streaming job | Job completes using actual scripts | Not run because Python 3 is missing; no HDFS test paths created | NOT RUN |
| Mapper in Hadoop | Executes correctly | Not run; Python 3 runtime unavailable | NOT RUN |
| Reducer in Hadoop | Correct ranking | Not run; Python 3 runtime unavailable | NOT RUN |
| Kafka producer | Publishes transactions | Producer from host environment failed with `KafkaTimeoutError` after 60s | FAIL |
| Kafka consumer | Receives transactions | Not executed because producer metadata failed | NOT RUN |
| HDFS ingestion | Transactions stored | Not executed because producer metadata failed | NOT RUN |
| Final CSV | Correct top 10 | Not produced in this session | NOT RUN |
| Scheduler | Expected behavior | Not executed because downstream pipeline not healthy | NOT RUN |

### Verified command evidence

Commands executed successfully and captured:

```text
docker --version
Docker version 29.6.2, build dfc4efb

docker compose version
Docker Compose version v5.3.1

docker compose config --services
kafka
namenode
datanode
pipeline

docker exec top-selling-product-ranking-kafka-1 bash -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
(no topics listed; exit=0)

docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
top-selling-product-ranking-datanode-1   Up 5 minutes (healthy)
top-selling-product-ranking-kafka-1      Up 5 minutes (healthy)
top-selling-product-ranking-namenode-1   Up 5 minutes (healthy)

curl -I http://localhost:9870
HTTP/1.1 200 OK

docker exec top-selling-product-ranking-namenode-1 bash -lc "command -v java; java -version; command -v python3; /opt/hadoop-3.2.1/bin/hadoop version; find /opt/hadoop-3.2.1/share/hadoop/tools/lib -maxdepth 1 -name 'hadoop-streaming-*.jar' -print"
/usr/bin/java
openjdk version "1.8.0_232"
bash: python3: command not found
Hadoop 3.2.1
/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar

docker exec top-selling-product-ranking-namenode-1 bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -ls /sales"
ls: `/sales': No such file or directory
exit=1
```

### Exact failure evidence

```text
E: Failed to fetch http://deb.debian.org/debian/dists/stretch/main/binary-amd64/Packages  404  Not Found
E: Failed to fetch http://deb.debian.org/debian/dists/stretch-updates/main/binary-amd64/Packages  404  Not Found
E: Failed to fetch http://security.debian.org/debian-security/dists/stretch/updates/main/binary-amd64/Packages  404  Not Found
```

and

```text
KafkaTimeoutError: Failed to update metadata after 60.0 secs.
```

These are the precise reasons the full Docker and end-to-end integration steps remain blocked.

The isolated Hadoop Streaming run is independently blocked because the actual Python mapper and reducer cannot execute without Python 3 in the Hadoop job container. Hadoop, Java, and the Streaming JAR were verified present; no HDFS test paths were created.

## 26. Final Commit Scope

The exact files that should be committed to `feature/mapreduce-ranking` are:

- `tests/test_mapreduce.py`
- `MEMBER3_MAPREDUCE_GUIDE.md`

No production code was modified as part of the Member 3 work. The existing implementation in `mapreduce/mapper.py` and `mapreduce/reducer.py` was left intact and reviewed only for compatibility with the actual pipeline contract.

## 27. How Teammates Can Reproduce and Validate After Merging

After the external Docker and Kafka issues are fixed, teammates can validate the project using the following commands:

```powershell
Set-Location "D:\top-selling-product-ranking"
python -m unittest discover -s tests -p "test_mapreduce.py" -v

docker compose up -d
docker ps
docker logs --tail=80 top-selling-product-ranking-kafka-1
docker logs --tail=80 top-selling-product-ranking-namenode-1

docker exec top-selling-product-ranking-namenode-1 bash -lc "hdfs dfs -ls /sales"
```

If the pipeline image is fixed and Kafka metadata is reachable, the next validation steps are:

```powershell
docker exec top-selling-product-ranking-namenode-1 bash -lc "hdfs dfs -ls /sales/input"
docker exec top-selling-product-ranking-namenode-1 bash -lc "hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -D mapreduce.framework.name=local -D mapreduce.job.reduces=1 -input /sales/input -output /sales/output/top-products -mapper \"python3 mapper.py\" -reducer \"python3 reducer.py\" -file /app/mapreduce/mapper.py -file /app/mapreduce/reducer.py"
docker exec top-selling-product-ranking-namenode-1 bash -lc "hdfs dfs -cat /sales/output/top-products/part-*"
```

The expected ranking for the supplied sample data remains:

```csv
rank,product_name,units_sold
1,Wireless Headphones,1842
2,Smart Watch,1560
3,Laptop Stand,1304
4,USB-C Hub,1048
5,Mechanical Keyboard,986
6,Bluetooth Speaker,872
7,Webcam,764
8,Portable SSD,693
9,Wireless Mouse,612
10,Phone Charger,548
```

## Final Note

The Member 3 local logic is validated and the local Python tests pass. The full Docker/Hadoop integration remains blocked by real runtime configuration issues outside the Member 3 scope, and those blockers were documented with actual evidence. No teammate source files or mapper/reducer logic were modified without approval.

## 24. Complete Kafka → HDFS → MapReduce Test Instructions

After approval:

1. start the Docker services
2. verify Kafka and Hadoop are healthy
3. confirm the topic exists
4. confirm HDFS input is present
5. run the producer/consumer flow
6. inspect HDFS stored JSON records
7. run Hadoop Streaming with the existing mapper and reducer
8. inspect the output under `/sales/output/top-products`
9. confirm final CSV values and ordering

## 25. Actual Integration Test Results

This section intentionally reflects the actual state of the project:

| Test | Expected | Actual | Status |
|---|---|---|---|
| Local Python tests | All pass | 14 tests run, all passed | PASS |
| Docker startup | Services run | Not executed yet | NOT RUN |
| HDFS availability | HDFS accessible | Not executed yet | NOT RUN |
| Hadoop Streaming job | Job completes | Not executed yet | NOT RUN |
| Mapper in Hadoop | Executes correctly | Not executed yet | NOT RUN |
| Reducer in Hadoop | Correct ranking | Not executed yet | NOT RUN |
| Kafka producer | Publishes transactions | Not executed yet | NOT RUN |
| Kafka consumer | Receives transactions | Not executed yet | NOT RUN |
| HDFS ingestion | Transactions stored | Not executed yet | NOT RUN |
| Final CSV | Correct top 10 | Not executed yet | NOT RUN |
| Scheduler | Expected behavior | Not executed yet | NOT RUN |

Test dates:

- Local Python tests executed in this session on 2026-10-11.
- Docker/Hadoop integration remains pending approval.

---

## Final Note

The Member 3 local logic is verified and the automated tests pass.

Real Docker/Hadoop integration remains pending approval and should be started only after confirmation to avoid altering shared or persistent data.
