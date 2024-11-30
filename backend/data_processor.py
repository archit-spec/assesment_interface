import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def process_mtr(file_content: bytes) -> pd.DataFrame:
    """Process MTR Sheet with proper DataFrame copies and error handling"""
    try:
        logger.info("Starting MTR processing")
        # Read Excel content
        df1 = pd.read_excel(file_content)

        # Create a copy of original DataFrame
        mtr_df = df1.copy()

        # Remove 'Cancel' rows
        initial_rows = len(mtr_df)
        mtr_df = mtr_df[mtr_df["Transaction Type"] != "Cancel"]
        logger.info(f"Removed {initial_rows - len(mtr_df)} Cancel transactions")

        # Create mapping for Transaction Type replacements
        type_mapping = {"Refund": "Return", "FreeReplacement": "Return"}

        # Apply replacements
        mtr_df.loc[:, "Transaction Type"] = mtr_df["Transaction Type"].replace(
            type_mapping
        )
        logger.info("Completed Transaction Type mappings")

        return mtr_df

    except Exception as e:
        logger.error(f"Error processing MTR data: {str(e)}")
        raise


def process_payment(file_content: bytes) -> pd.DataFrame:
    """Process Payment Report with proper DataFrame copies and error handling"""
    try:
        logger.info("Starting Payment Report processing")
        # Read CSV content
        df2 = pd.read_csv(file_content)

        # Create a copy of original DataFrame
        payment_df = df2.copy()

        # Remove 'Transfer' rows
        initial_rows = len(payment_df)
        payment_df = payment_df[payment_df["Type"] != "Transfer"]
        logger.info(f"Removed {initial_rows - len(payment_df)} Transfer transactions")

        # Rename 'Type' column to 'Payment Type'
        payment_df = payment_df.rename(columns={"Type": "Payment Type"})

        # Create mapping for Payment Type replacements
        value_mapping = {
            "Adjustment": "Order",
            "FBA Inventory Fee": "Order",
            "Fulfilment Fee Refund": "Order",
            "Service Fee": "Order",
            "Refund": "Return",
        }

        # Apply replacements
        payment_df.loc[:, "Payment Type"] = payment_df["Payment Type"].replace(
            value_mapping
        )

        # Add Transaction Type column
        payment_df.loc[:, "Transaction Type"] = "Payment"

        logger.info("Completed Payment Type mappings and added Transaction Type")
        return payment_df

    except Exception as e:
        logger.error(f"Error processing Payment data: {str(e)}")
        raise


def merge_datasets(mtr_df: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
    """Merge processed datasets with error handling"""
    try:
        logger.info("Starting dataset merge")
        # Create copies
        mtr = mtr_df.copy()
        payment = payment_df.copy()

        # Rename payment columns to match MTR
        payment = payment.rename(
            columns={
                "Order ID": "Order Id",
                "Description": "P_Description",
                "Amount": "Net Amount",
            }
        )

        # Merge on Order Id
        merged_df = pd.merge(mtr, payment, on="Order Id", how="outer")
        logger.info(f"Merged dataset created with {len(merged_df)} rows")
        return merged_df

    except Exception as e:
        logger.error(f"Error merging datasets: {str(e)}")
        raise


def process_merged_data(merged_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Process merged dataset with error handling"""
    try:
        logger.info("Starting merged data processing")
        results = {}

        # Filter valid orders
        valid_orders = merged_df[merged_df["Order Id"].notna()].copy()
        logger.info(f"Found {len(valid_orders)} valid orders")

        # Process each category
        results["removal_orders"] = valid_orders[
            valid_orders["Order Id"].str.len() == 10
        ].copy()
        logger.info(f"Found {len(results['removal_orders'])} removal orders")

        results["returns"] = valid_orders[
            (valid_orders["Transaction Type"] == "Return")
            & (valid_orders["Invoice Amount"].notna())
        ].copy()
        logger.info(f"Found {len(results['returns'])} returns")

        results["negative_payout"] = valid_orders[
            (valid_orders["Transaction Type"] == "Payment")
            & (valid_orders["Net Amount"] < 0)
        ].copy()
        logger.info(f"Found {len(results['negative_payout'])} negative payouts")

        results["order_payment_received"] = valid_orders[
            (valid_orders["Net Amount"].notna())
            & (valid_orders["Invoice Amount"].notna())
        ].copy()
        logger.info(
            f"Found {len(results['order_payment_received'])} orders with payment received"
        )

        results["order_not_applicable"] = valid_orders[
            (valid_orders["Net Amount"].notna())
            & (valid_orders["Invoice Amount"].isna())
        ].copy()
        logger.info(
            f"Found {len(results['order_not_applicable'])} not applicable orders"
        )

        results["payment_pending"] = valid_orders[
            (valid_orders["Net Amount"].isna())
            & (valid_orders["Invoice Amount"].notna())
        ].copy()
        logger.info(f"Found {len(results['payment_pending'])} pending payments")

        return results

    except Exception as e:
        logger.error(f"Error processing merged data: {str(e)}")
        raise


def calculate_tolerance(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate tolerance with error handling"""
    try:
        logger.info("Starting tolerance calculation")
        # Create a copy
        tolerance_df = df.copy()

        def check_tolerance(row):
            pna = row["Net Amount"]
            if (
                pd.isna(pna)
                or pd.isna(row["Invoice Amount"])
                or row["Invoice Amount"] == 0
            ):
                return None

            percentage = (pna / row["Invoice Amount"]) * 100

            if 0 < pna <= 300:
                return "Within Tolerance" if percentage > 50 else "Tolerance Breached"
            elif 300 < pna <= 500:
                return "Within Tolerance" if percentage > 45 else "Tolerance Breached"
            elif 500 < pna <= 900:
                return "Within Tolerance" if percentage > 43 else "Tolerance Breached"
            elif 900 < pna <= 1500:
                return "Within Tolerance" if percentage > 38 else "Tolerance Breached"
            elif pna > 1500:
                return "Within Tolerance" if percentage > 30 else "Tolerance Breached"
            return None

        tolerance_df["Tolerance_Status"] = tolerance_df.apply(check_tolerance, axis=1)

        # Log tolerance results
        tolerance_counts = tolerance_df["Tolerance_Status"].value_counts()
        logger.info(
            f"Tolerance calculation complete. Results: {tolerance_counts.to_dict()}"
        )

        return tolerance_df

    except Exception as e:
        logger.error(f"Error calculating tolerance: {str(e)}")
        raise


def process_files(mtr_content: bytes, payment_content: bytes) -> Dict[str, Any]:
    """Main processing pipeline with error handling"""
    try:
        logger.info("Starting main processing pipeline")

        # Process MTR data
        logger.info("Processing MTR data...")
        mtr_processed = process_mtr(mtr_content)

        # Process Payment data
        logger.info("Processing Payment data...")
        payment_processed = process_payment(payment_content)

        # Merge datasets
        logger.info("Merging datasets...")
        merged_df = merge_datasets(mtr_processed, payment_processed)

        # Process merged data
        logger.info("Processing merged data...")
        categorized_data = process_merged_data(merged_df)

        # Calculate tolerance
        logger.info("Calculating tolerance...")
        tolerance_checked = calculate_tolerance(merged_df)

        # Calculate summary statistics
        total_transactions = len(merged_df)
        total_invoice_amount = merged_df["Invoice Amount"].sum()
        total_net_amount = merged_df["Net Amount"].sum()

        # Prepare transactions by date
        merged_df["Date"] = pd.to_datetime(merged_df["Date"])
        transactions_by_date = (
            merged_df.groupby(merged_df["Date"].dt.date.astype(str))
            .agg({"Net Amount": "sum", "Order Id": "count"})
            .reset_index()
            .rename(
                columns={"Date": "date", "Net Amount": "amount", "Order Id": "count"}
            )
        ).to_dict("records")

        # Prepare transactions by type
        transactions_by_type = (
            merged_df.groupby("Transaction Type")
            .agg({"Net Amount": "sum", "Order Id": "count"})
            .reset_index()
            .rename(
                columns={
                    "Transaction Type": "type",
                    "Net Amount": "amount",
                    "Order Id": "count",
                }
            )
        ).to_dict("records")

        # Prepare response data
        response_data = {
            "summary": {
                "count": int(total_transactions),
                "invoiceAmount": float(total_invoice_amount),
                "netAmount": float(total_net_amount),
            },
            "charts": {
                "transactionsByDate": transactions_by_date,
                "transactionTypes": transactions_by_type,
            },
            "categories": {
                category: {
                    "count": len(data),
                    "invoice_amount": float(data["Invoice Amount"].sum())
                    if "Invoice Amount" in data
                    else 0,
                    "net_amount": float(data["Net Amount"].sum())
                    if "Net Amount" in data
                    else 0,
                }
                for category, data in categorized_data.items()
            },
            "tolerance": {
                status: int(count)
                for status, count in tolerance_checked["Tolerance_Status"]
                .value_counts()
                .items()
                if pd.notna(status)
            },
        }

        logger.info("Processing pipeline complete")
        return response_data

    except Exception as e:
        logger.error(f"Error in main processing pipeline: {str(e)}")
        raise
