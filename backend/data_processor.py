"""Data processing module for handling various file formats.

This module contains functions for processing different types of data files,
including MTR and payment files. It handles data validation, transformation,
and standardization.
"""

import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def process_mtr_file(file_path: str) -> pd.DataFrame:
    """Process MTR file data.

    Args:
        file_path: Path to the MTR file (Excel or CSV).

    Returns:
        Processed DataFrame.
    """
    try:
        logger.info(f"Processing MTR file: {file_path}")
        
        # Verify file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"MTR file not found: {file_path}")
            
        # Try reading the file based on extension
        file_ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if file_ext == '.csv':
                df = pd.read_csv(file_path, quoting=1)  # QUOTE_ALL to handle newlines
            else:
                # Try different Excel engines
                excel_engines = ['openpyxl', 'xlrd']
                last_error = None
                
                for engine in excel_engines:
                    try:
                        df = pd.read_excel(file_path, engine=engine)
                        logger.info(f"Successfully read Excel file using {engine} engine")
                        break
                    except ImportError as e:
                        last_error = e
                        continue
                    except Exception as e:
                        last_error = e
                        continue
                else:
                    raise ImportError(f"Failed to read Excel file with any engine. Last error: {last_error}")
                
            logger.info(f"Successfully read MTR file with {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            raise
        
        # Create a copy
        mtr_df = df.copy()
        
        # Clean column names
        mtr_df.columns = mtr_df.columns.str.strip()
        
        # Log initial data info
        logger.info(f"MTR columns: {mtr_df.columns.tolist()}")
        logger.info(f"Initial row count: {len(mtr_df)}")
        
        # Ensure required columns exist
        required_columns = ["Transaction Type", "Order Id", "Invoice Amount"]
        missing_columns = [col for col in required_columns if col not in mtr_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Remove Cancel rows
        initial_rows = len(mtr_df)
        mtr_df = mtr_df[mtr_df["Transaction Type"] != "Cancel"]
        logger.info(f"Removed {initial_rows - len(mtr_df)} Cancel transactions")
        
        # Map Transaction Type values
        type_mapping = {
            "Refund": "Return",
            "FreeReplacement": "Return"
        }
        mtr_df["Transaction Type"] = mtr_df["Transaction Type"].replace(type_mapping)
        logger.info("Mapped Transaction Types")
        
        # Convert Invoice Amount to float if it's not already
        if mtr_df["Invoice Amount"].dtype != np.float64:
            try:
                mtr_df["Invoice Amount"] = pd.to_numeric(
                    mtr_df["Invoice Amount"].astype(str).str.replace(',', ''),
                    errors='coerce'
                )
            except Exception as e:
                logger.error(f"Error converting Invoice Amount to float: {str(e)}")
                raise
        
        # Log final data info
        logger.info(f"Final row count: {len(mtr_df)}")
        logger.info(f"Transaction Types: {mtr_df['Transaction Type'].unique().tolist()}")
        
        return mtr_df

    except Exception as e:
        logger.error(f"Error processing MTR file: {str(e)}")
        raise


def process_payment_file(file_path: str) -> pd.DataFrame:
    """Process payment file data.

    Args:
        file_path: Path to the payment CSV file.

    Returns:
        Processed DataFrame.
    """
    try:
        logger.info(f"Processing payment file: {file_path}")
        
        # Verify file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Payment file not found: {file_path}")
            
        # Read CSV file with special handling for newlines in cells
        try:
            df = pd.read_csv(file_path, quoting=1)  # QUOTE_ALL to handle newlines in cells
            logger.info(f"Successfully read payment file with {len(df)} rows")
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            raise
        
        # Create a copy
        payment_df = df.copy()
        
        # Clean column names
        payment_df.columns = payment_df.columns.str.strip().str.lower()
        
        # Clean Type column - remove newlines and whitespace
        payment_df['type'] = payment_df['type'].str.strip().str.replace('\n', '')
        
        # Log initial data info
        logger.info(f"Payment columns: {payment_df.columns.tolist()}")
        logger.info(f"Initial row count: {len(payment_df)}")
        
        # Remove Transfer rows
        initial_rows = len(payment_df)
        payment_df = payment_df[payment_df["type"] != "Transfer"]
        logger.info(f"Removed {initial_rows - len(payment_df)} Transfer rows")
        
        # Rename columns
        payment_df = payment_df.rename(columns={
            "order id": "Order Id",
            "description": "P_Description",
            "total": "Net Amount",
            "type": "Payment Type"
        })
        
        # Clean Net Amount - remove commas and convert to float
        payment_df["Net Amount"] = payment_df["Net Amount"].str.replace(',', '').astype(float)
        
        # Map Payment Type values
        value_mapping = {
            "Adjustment": "Order",
            "FBA Inventory Fee": "Order",
            "Fulfilment Fee Refund": "Order",
            "Service Fee": "Order",
            "Refund": "Return"
        }
        payment_df["Payment Type"] = payment_df["Payment Type"].replace(value_mapping)
        
        # Add Transaction Type column
        payment_df["Transaction Type"] = "Payment"
        logger.info("Added Transaction Type column")
        
        # Log final data info
        logger.info(f"Final row count: {len(payment_df)}")
        logger.info(f"Payment Types: {payment_df['Payment Type'].unique().tolist()}")
        
        return payment_df

    except Exception as e:
        logger.error(f"Error processing payment file: {str(e)}")
        raise


def merge_datasets(mtr_df: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
    """Merge MTR and payment datasets.

    Args:
        mtr_df: Processed MTR DataFrame.
        payment_df: Processed payment DataFrame.

    Returns:
        Merged DataFrame.
    """
    try:
        logger.info("Merging datasets")
        
        # Create copies
        mtr = mtr_df.copy()
        payment = payment_df.copy()
        
        # Log pre-merge info
        logger.info(f"MTR columns: {mtr.columns.tolist()}")
        logger.info(f"Payment columns: {payment.columns.tolist()}")
        
        # Rename payment columns to match MTR
        payment = payment.rename(columns={
            "Order ID": "Order Id",
            "Description": "P_Description",
            "Amount": "Net Amount"
        })
        
        # Merge on Order Id
        merged_df = pd.merge(mtr, payment, on="Order Id", how="outer")
        logger.info(f"Created merged dataset with {len(merged_df)} rows")
        logger.info(f"Merged columns: {merged_df.columns.tolist()}")
        
        return merged_df

    except Exception as e:
        logger.error(f"Error merging datasets: {str(e)}")
        raise


def process_merged_data(merged_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Process merged dataset into categories.

    Args:
        merged_df: Merged DataFrame.

    Returns:
        Dictionary containing categorized DataFrames.
    """
    try:
        logger.info("Processing merged data")
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
            (valid_orders["Transaction Type"] == "Return") & 
            (valid_orders["Invoice Amount"].notna())
        ].copy()
        logger.info(f"Found {len(results['returns'])} returns")
        
        results["negative_payout"] = valid_orders[
            (valid_orders["Transaction Type"] == "Payment") & 
            (valid_orders["Net Amount"] < 0)
        ].copy()
        logger.info(f"Found {len(results['negative_payout'])} negative payouts")
        
        results["order_payment_received"] = valid_orders[
            (valid_orders["Net Amount"].notna()) & 
            (valid_orders["Invoice Amount"].notna())
        ].copy()
        logger.info(
            f"Found {len(results['order_payment_received'])} orders with payment"
        )
        
        results["order_not_applicable"] = valid_orders[
            (valid_orders["Net Amount"].notna()) & 
            (valid_orders["Invoice Amount"].isna())
        ].copy()
        logger.info(
            f"Found {len(results['order_not_applicable'])} not applicable orders"
        )
        
        results["payment_pending"] = valid_orders[
            (valid_orders["Net Amount"].isna()) & 
            (valid_orders["Invoice Amount"].notna())
        ].copy()
        logger.info(f"Found {len(results['payment_pending'])} pending payments")
        
        return results

    except Exception as e:
        logger.error(f"Error processing merged data: {str(e)}")
        raise


def calculate_tolerance(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate tolerance status for orders.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with tolerance status.
    """
    try:
        logger.info("Calculating tolerance")
        tolerance_df = df.copy()
        
        def check_tolerance(row):
            pna = row["Net Amount"]
            if pd.isna(pna) or pd.isna(row["Invoice Amount"]) or row["Invoice Amount"] == 0:
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
        logger.info(f"Tolerance calculation complete: {tolerance_counts.to_dict()}")
        
        return tolerance_df

    except Exception as e:
        logger.error(f"Error calculating tolerance: {str(e)}")
        raise


def process_files(mtr_path: str, payment_path: str) -> Dict[str, Any]:
    """Process MTR and payment files and generate analysis.

    Args:
        mtr_path: Path to MTR Excel file.
        payment_path: Path to payment CSV file.

    Returns:
        Dictionary containing processed data and summary statistics.
    """
    try:
        logger.info("Starting main processing pipeline")
        logger.info(f"MTR file: {mtr_path}")
        logger.info(f"Payment file: {payment_path}")
        
        # Process MTR data
        logger.info("Processing MTR data...")
        mtr_processed = process_mtr_file(mtr_path)
        
        # Process Payment data
        logger.info("Processing Payment data...")
        payment_processed = process_payment_file(payment_path)
        
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
            .rename(columns={
                "Date": "date",
                "Net Amount": "amount",
                "Order Id": "count"
            })
        ).to_dict("records")
        
        # Prepare transactions by type
        transactions_by_type = (
            merged_df.groupby("Transaction Type")
            .agg({"Net Amount": "sum", "Order Id": "count"})
            .reset_index()
            .rename(columns={
                "Transaction Type": "type",
                "Net Amount": "amount",
                "Order Id": "count"
            })
        ).to_dict("records")
        
        # Prepare response data
        response_data = {
            "summary": {
                "count": int(total_transactions),
                "invoiceAmount": float(total_invoice_amount),
                "netAmount": float(total_net_amount)
            },
            "charts": {
                "transactionsByDate": transactions_by_date,
                "transactionTypes": transactions_by_type
            },
            "categories": {
                category: {
                    "count": len(data),
                    "invoice_amount": float(data["Invoice Amount"].sum())
                    if "Invoice Amount" in data else 0,
                    "net_amount": float(data["Net Amount"].sum())
                    if "Net Amount" in data else 0
                }
                for category, data in categorized_data.items()
            },
            "tolerance": {
                status: int(count)
                for status, count in tolerance_checked["Tolerance_Status"]
                .value_counts()
                .items()
                if pd.notna(status)
            }
        }
        
        logger.info("Processing pipeline complete")
        return response_data

    except Exception as e:
        logger.error(f"Error in main processing pipeline: {str(e)}")
        raise
