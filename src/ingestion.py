import csv
import yaml
import logging
import sys


# -------------------------------
# Load configuration
# -------------------------------
def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# -------------------------------
# Setup logging
# -------------------------------
def setup_logging(level):
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


# -------------------------------
# Stream CSV rows safely
# -------------------------------
def csv_reader(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # normalize headers + values
            yield {
                k.strip().lower(): (v.strip() if v else "")
                for k, v in row.items()
            }


# -------------------------------
# Validation logic (FINAL)
# -------------------------------
def is_valid_row(row, rules):
    email = row.get("email", "")
    age = row.get("age", "")
    country = row.get("country", "")

    if rules.get("email_required") and "@" not in email:
        return False

    if rules.get("age_positive"):
        if not age.isdigit() or int(age) <= 0:
            return False

    if rules.get("country_required") and not country:
        return False

    return True


# -------------------------------
# Read → Validate → Route
# -------------------------------
def process_file(config):
    input_file = config["input"]["filename"]
    clean_path = config["output"]["clean_file"]
    reject_path = config["output"]["reject_file"]
    rules = config["validation"]

    total = valid = invalid = 0
    clean_writer = reject_writer = None

    logging.info("Starting ingestion pipeline")
    logging.info("Input file: %s", input_file)

    with open(clean_path, "w", newline="", encoding="utf-8") as clean_f, \
         open(reject_path, "w", newline="", encoding="utf-8") as reject_f:

        for row in csv_reader(input_file):
            total += 1

            if is_valid_row(row, rules):
                if clean_writer is None:
                    clean_writer = csv.DictWriter(clean_f, fieldnames=row.keys())
                    clean_writer.writeheader()
                clean_writer.writerow(row)
                valid += 1
            else:
                if reject_writer is None:
                    reject_writer = csv.DictWriter(reject_f, fieldnames=row.keys())
                    reject_writer.writeheader()
                reject_writer.writerow(row)
                invalid += 1

    logging.info("Pipeline completed")
    logging.info("Total rows   : %d", total)
    logging.info("Valid rows   : %d", valid)
    logging.info("Invalid rows : %d", invalid)


# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python src/ingestion.py config/config.yaml")
        sys.exit(1)

    config = load_config(sys.argv[1])
    setup_logging(config["logging"]["level"])
    process_file(config)
