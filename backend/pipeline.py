import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any, List
from models import UnprocessedData, ProcessedData, get_db
from sqlalchemy.orm import Session
import logging
from config import BATCH_SIZE

logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self, db: Session):
        self.db = db

    async def extract(self, file_path: str, file_type: str) -> UnprocessedData:
        """Extract data from file and store in unprocessed table"""
        try:
            # Read CSV file with specific handling for each type
            if file_type == "mtr":
                df = pd.read_excel(
                    file_path,
                    parse_dates=["Invoice Date", "Shipment Date", "Order Date"],
                )
                # Clean up column names
                df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            else:  # payment
                # Read payment CSV with specific options for handling quoted values and newlines
                df = pd.read_csv(
                    file_path,
                    parse_dates=["date/time"],
                    quoting=1,  # QUOTE_ALL - handle quoted values properly
                    thousands=",",  # Handle numbers with commas
                )

                # Clean up column names - convert to snake_case
                df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

                # Clean up type values by removing newlines and extra whitespace
                if "type" in df.columns:
                    df["type"] = df["type"].str.replace("\n", "").str.strip()

            # Convert DataFrame to JSON
            raw_data = json.loads(df.to_json(orient="records", date_format="iso"))

            # Create unprocessed record
            unprocessed = UnprocessedData(
                filename=file_path.split("/")[-1],
                file_type=file_type,
                raw_data=raw_data,
                status="pending",
            )
            self.db.add(unprocessed)
            self.db.commit()
            self.db.refresh(unprocessed)

            return unprocessed
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise

    def transform_mtr(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform MTR data"""
        df = pd.DataFrame(data)

        # Specific transformations for MTR data
        df = df[df["transaction_type"] != "Cancel"]  # Remove Cancel rows

        # Convert amount columns to numeric, handling currency formatting
        if "invoice_amount" in df.columns:
            df["invoice_amount"] = pd.to_numeric(
                df["invoice_amount"].astype(str).str.replace(",", ""), errors="coerce"
            )

        # Standardize transaction types
        df["transaction_type"] = df["transaction_type"].replace(
            {"Refund": "Return", "FreeReplacement": "Return"}
        )

        # Add processing metadata
        df["processed_date"] = datetime.utcnow().isoformat()
        df["source"] = "mtr"

        return json.loads(df.to_json(orient="records", date_format="iso"))

    def transform_payment(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform payment data"""
        try:
            df = pd.DataFrame(data)
            logger.info(f"Payment data columns: {df.columns.tolist()}")

            # Clean up column names - convert to lowercase and remove whitespace
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

            # Clean up type values
            if "type" in df.columns:
                df["type"] = df["type"].str.replace("\n", "").str.strip()

            # Convert total to numeric
            if "total" in df.columns:
                # Handle any remaining formatting in total column
                df["total"] = (
                    df["total"]
                    .astype(str)
                    .str.replace("$", "")
                    .str.replace(",", "")
                    .str.strip()
                )
                df["total"] = pd.to_numeric(df["total"], errors="coerce")

            # Remove Transfer rows
            if "type" in df.columns:
                df = df[df["type"] != "Transfer"]

                # Standardize transaction types
                df["type"] = df["type"].replace(
                    {
                        "Adjustment": "Order",
                        "FBA Inventory Fee": "Order",
                        "Fulfilment Fee Refund": "Order",
                        "Service Fee": "Order",
                        "Refund": "Return",
                    }
                )

            # Add processing metadata
            df["processed_date"] = datetime.utcnow().isoformat()
            df["source"] = "payment"

            return json.loads(df.to_json(orient="records", date_format="iso"))

        except Exception as e:
            logger.error(f"Payment transformation failed: {str(e)}")
            raise

    def calculate_summary(
        self, data: List[Dict[str, Any]], file_type: str
    ) -> Dict[str, Any]:
        """Calculate summary statistics"""
        try:
            df = pd.DataFrame(data)
            logger.info(f"Calculating summary for {file_type} data")
            logger.info(f"DataFrame info: {df.info()}")
            logger.info(f"DataFrame head: {df.head().to_dict()}")

            summary = {
                "total_records": len(df),
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Determine amount column based on file type
            if file_type == "mtr":
                amount_col = "invoice_amount"
                date_col = "invoice_date"
                type_col = "transaction_type"
            else:  # payment
                amount_col = "total"
                date_col = "date/time"
                type_col = "type"

            logger.info(f"Amount column: {amount_col}")
            logger.info(f"Column dtypes: {df.dtypes.to_dict()}")

            # Handle amount calculations
            if amount_col in df.columns:
                logger.info(f"Raw {amount_col} values: {df[amount_col].head()}")

                # Convert to numeric if not already
                if not pd.api.types.is_numeric_dtype(df[amount_col]):
                    # First clean the data
                    cleaned_values = (
                        df[amount_col]
                        .astype(str)
                        .str.replace("$", "")
                        .str.replace(",", "")
                    )
                    logger.info(f"Cleaned values: {cleaned_values.head()}")

                    # Then convert to numeric
                    df[amount_col] = pd.to_numeric(cleaned_values, errors="coerce")
                    logger.info(f"Numeric values: {df[amount_col].head()}")

                # Remove NaN values for calculations
                amount_series = df[amount_col].dropna()
                logger.info(f"Non-null values count: {len(amount_series)}")

                if not amount_series.empty:
                    summary.update(
                        {
                            "total_amount": float(amount_series.sum()),
                            "average_amount": float(amount_series.mean()),
                            "max_amount": float(amount_series.max()),
                            "min_amount": float(amount_series.min()),
                        }
                    )
                else:
                    logger.warning("No valid numeric values found in amount column")
                    summary.update(
                        {
                            "total_amount": 0.0,
                            "average_amount": 0.0,
                            "max_amount": 0.0,
                            "min_amount": 0.0,
                        }
                    )
            else:
                logger.error(
                    f"Amount column {amount_col} not found in columns: {df.columns.tolist()}"
                )

            # Add transaction type breakdown
            if type_col in df.columns:
                logger.info(f"Unique types: {df[type_col].unique()}")

                # Group by type and calculate stats
                type_stats = (
                    df.groupby(type_col)[amount_col].agg(["count", "sum"]).round(2)
                )
                logger.info(f"Type summary: {type_stats.to_dict()}")

                # Convert to dictionary format
                summary["transaction_types"] = {
                    str(ttype): {
                        "count": int(row["count"]),
                        "total_amount": float(row["sum"]),
                    }
                    for ttype, row in type_stats.iterrows()
                }
            else:
                logger.error(
                    f"Type column {type_col} not found in columns: {df.columns.tolist()}"
                )

            logger.info(f"Final summary: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Summary calculation failed: {str(e)}")
            logger.error(f"Data sample: {data[:2] if data else 'No data'}")
            raise

    async def load(
        self,
        unprocessed_id: int,
        processed_data: List[Dict[str, Any]],
        summary: Dict[str, Any],
        file_type: str,
    ) -> ProcessedData:
        """Load processed data into processed table"""
        try:
            processed = ProcessedData(
                unprocessed_id=unprocessed_id,
                processed_data=processed_data,
                summary=summary,
                status="completed",
                file_type=file_type,
            )
            self.db.add(processed)
            self.db.commit()
            self.db.refresh(processed)

            # Update unprocessed record status
            unprocessed = (
                self.db.query(UnprocessedData)
                .filter(UnprocessedData.id == unprocessed_id)
                .first()
            )
            if unprocessed:
                unprocessed.status = "processed"
                self.db.commit()

            return processed
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise

    async def process_file(self, unprocessed_id: int) -> ProcessedData:
        """Process a single file through the ETL pipeline"""
        try:
            # Get unprocessed record
            unprocessed = (
                self.db.query(UnprocessedData)
                .filter(UnprocessedData.id == unprocessed_id)
                .first()
            )

            if not unprocessed:
                raise ValueError(
                    f"No unprocessed record found with id {unprocessed_id}"
                )

            # Update status to processing
            unprocessed.status = "processing"
            self.db.commit()

            # Transform data based on file type
            if unprocessed.file_type == "mtr":
                processed_data = self.transform_mtr(unprocessed.raw_data)
            else:  # payment
                processed_data = self.transform_payment(unprocessed.raw_data)

            # Calculate summary
            summary = self.calculate_summary(processed_data, unprocessed.file_type)

            # Load processed data
            processed = await self.load(
                unprocessed_id=unprocessed.id,
                processed_data=processed_data,
                summary=summary,
                file_type=unprocessed.file_type,
            )

            return processed
        except Exception as e:
            # Update status to failed
            if unprocessed:
                unprocessed.status = "failed"
                unprocessed.error_message = str(e)
                self.db.commit()
            logger.error(f"Processing failed: {str(e)}")
            raise
