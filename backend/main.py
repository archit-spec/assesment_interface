from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc
import pandas as pd
from datetime import datetime, date
from typing import List, Optional, Dict
import logging
import json
from pathlib import Path
import shutil
import os
import uuid
from models import (
    UnprocessedData, 
    ProcessedData, 
    UnprocessedDataResponse, 
    ProcessedDataResponse,
    PaginatedResponse,
    TransactionSummary,
    TransactionQuery,
    get_db
)
from pipeline import ETLPipeline
from config import UPLOAD_DIR, API_HOST, API_PORT, DEBUG_MODE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Global storage for processing jobs
processing_jobs: Dict[str, dict] = {}

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload/mtr", response_model=UnprocessedDataResponse)
async def upload_mtr(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload and process MTR file"""
    try:
        # Log request details
        logger.info(f"Received MTR file upload request: {file.filename}")
        
        # Validate file extension
        if not file.filename.endswith('.csv'):
            error_msg = "File must be a CSV file"
            logger.error(f"File validation failed: {error_msg}")
            raise HTTPException(400, error_msg)

        # Create upload directory if it doesn't exist
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        
        # Save file
        file_path = UPLOAD_DIR / f"mtr_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        logger.info(f"Saving file to: {file_path}")
        
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        except Exception as e:
            error_msg = f"Failed to save file: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(500, error_msg)
        
        # Extract and process data
        try:
            pipeline = ETLPipeline(db)
            logger.info("Extracting data from file")
            unprocessed = await pipeline.extract(str(file_path), 'mtr')
            
            logger.info("Starting file processing")
            processed = await pipeline.process_file(unprocessed.id)
            
            return UnprocessedDataResponse(
                id=unprocessed.id,
                filename=unprocessed.filename,
                file_type=unprocessed.file_type,
                upload_timestamp=unprocessed.upload_timestamp,
                status=unprocessed.status,
                error_message=unprocessed.error_message
            )
        except Exception as e:
            error_msg = f"Failed to process file: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(500, error_msg)
            
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Unexpected error during MTR file upload: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(500, error_msg)
    finally:
        # Clean up the temporary file
        if 'file_path' in locals():
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary file: {str(e)}")

@app.post("/upload/payment", response_model=UnprocessedDataResponse)
async def upload_payment(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Upload and process payment file"""
    try:
        # Log request details
        logger.info(f"Received payment file upload request: {file.filename}")
        
        # Validate file extension
        if not file.filename.endswith('.csv'):
            error_msg = "File must be a CSV file"
            logger.error(f"File validation failed: {error_msg}")
            raise HTTPException(400, error_msg)

        # Create upload directory if it doesn't exist
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        
        # Save file
        file_path = UPLOAD_DIR / f"payment_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        logger.info(f"Saving file to: {file_path}")
        
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        except Exception as e:
            error_msg = f"Failed to save file: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(500, error_msg)
        
        # Extract and process data
        try:
            pipeline = ETLPipeline(db)
            logger.info("Extracting data from file")
            unprocessed = await pipeline.extract(str(file_path), 'payment')
            
            logger.info("Starting file processing")
            processed = await pipeline.process_file(unprocessed.id)
            
            return UnprocessedDataResponse(
                id=unprocessed.id,
                filename=unprocessed.filename,
                file_type=unprocessed.file_type,
                upload_timestamp=unprocessed.upload_timestamp,
                status=unprocessed.status,
                error_message=unprocessed.error_message
            )
        except Exception as e:
            error_msg = f"Failed to process file: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(500, error_msg)
            
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Unexpected error during payment file upload: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(500, error_msg)
    finally:
        # Clean up the temporary file
        if 'file_path' in locals():
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary file: {str(e)}")

@app.get("/unprocessed", response_model=PaginatedResponse)
async def list_unprocessed(
    page: int = Query(1, gt=0),
    size: int = Query(10, gt=0, le=300),
    file_type: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List unprocessed files with pagination"""
    query = db.query(UnprocessedData)
    
    if file_type:
        query = query.filter(UnprocessedData.file_type == file_type)
    if status:
        query = query.filter(UnprocessedData.status == status)
        
    total = query.count()
    items = query.offset((page - 1) * size).limit(size).all()
    
    return PaginatedResponse(
        items=[UnprocessedDataResponse.from_orm(item) for item in items],
        total=total,
        page=page,
        size=size,
        pages=(total + size - 1) // size
    )

@app.get("/processed", response_model=PaginatedResponse)
async def list_processed(
    page: int = Query(1, gt=0),
    size: int = Query(10, gt=0, le=300),
    file_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """List processed files with pagination"""
    query = db.query(ProcessedData)
    
    if file_type:
        query = query.filter(ProcessedData.file_type == file_type)
        
    total = query.count()
    items = query.offset((page - 1) * size).limit(size).all()
    
    return PaginatedResponse(
        items=[ProcessedDataResponse.from_orm(item) for item in items],
        total=total,
        page=page,
        size=size,
        pages=(total + size - 1) // size
    )

@app.get("/processed/{id}", response_model=ProcessedDataResponse)
async def get_processed(id: int, db: Session = Depends(get_db)):
    """Get a specific processed file by ID"""
    processed = db.query(ProcessedData).filter(ProcessedData.id == id).first()
    if not processed:
        raise HTTPException(404, "Processed file not found")
    return ProcessedDataResponse.from_orm(processed)

@app.get("/api/summary", response_model=TransactionSummary)
async def get_summary(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    transaction_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get summary statistics for processed transactions"""
    try:
        # Get all processed records
        query = db.query(ProcessedData)
        
        # Apply filters if provided
        if transaction_type:
            query = query.filter(ProcessedData.file_type == transaction_type)
            
        records = query.order_by(desc(ProcessedData.processing_timestamp)).all()
        
        if not records:
            raise HTTPException(404, "No processed records found")
            
        # Combine all summaries
        total_records = 0
        total_amount = 0
        transaction_types = {}
        
        for record in records:
            summary = record.summary
            total_records += summary.get('total_records', 0)
            total_amount += summary.get('total_amount', 0)
            
            # Combine transaction type stats
            for ttype, stats in summary.get('transaction_types', {}).items():
                if ttype not in transaction_types:
                    transaction_types[ttype] = {'count': 0, 'total_amount': 0}
                transaction_types[ttype]['count'] += stats['count']
                transaction_types[ttype]['total_amount'] += stats['total_amount']
        
        return {
            'total_records': total_records,
            'total_amount': total_amount,
            'transaction_types': transaction_types,
            'start_date': start_date,
            'end_date': end_date
        }
    except Exception as e:
        logger.error(f"Failed to get summary: {str(e)}")
        raise HTTPException(500, f"Failed to get summary: {str(e)}")

@app.get("/api/transactions", response_model=PaginatedResponse)
async def get_transactions(
    page: int = Query(1, gt=0),
    size: int = Query(50, gt=0, le=1000),
    db: Session = Depends(get_db)
):
    """Get paginated transactions from all processed files"""
    try:
        # Get all processed records
        records = db.query(ProcessedData).order_by(desc(ProcessedData.processing_timestamp)).all()
        
        # Combine all transactions
        all_transactions = []
        for record in records:
            if record.processed_data:
                all_transactions.extend(record.processed_data)
        
        # Sort transactions by date (newest first)
        all_transactions.sort(key=lambda x: datetime.strptime(x.get('date/time', '1/1/1900'), '%m/%d/%Y'), reverse=True)
        
        # Calculate pagination
        total = len(all_transactions)
        start_idx = (page - 1) * size
        end_idx = min(start_idx + size, total)
        
        # Get page of transactions
        page_transactions = all_transactions[start_idx:end_idx]
        
        return {
            'items': page_transactions,
            'total': total,
            'page': page,
            'size': size,
            'pages': (total + size - 1) // size
        }
    except Exception as e:
        logger.error(f"Failed to get transactions: {str(e)}")
        raise HTTPException(500, f"Failed to get transactions: {str(e)}")

@app.get("/api/transaction/{order_id}")
async def get_transaction(
    order_id: str,
    db: Session = Depends(get_db)
):
    """Get detailed information for a specific transaction"""
    try:
        # Search through processed records for the order
        records = db.query(ProcessedData).all()
        
        for record in records:
            data = record.processed_data
            for transaction in data:
                if transaction.get('order_id') == order_id:
                    return transaction
                    
        raise HTTPException(404, f"Transaction with order_id {order_id} not found")
    except Exception as e:
        logger.error(f"Failed to get transaction: {str(e)}")
        raise HTTPException(500, f"Failed to get transaction: {str(e)}")

@app.post("/api/upload/{report_type}")
async def upload_file(report_type: str, file: UploadFile = File(...)):
    """Handle file upload and initiate processing"""
    try:
        # Validate file type
        if report_type not in ['payment', 'mtr']:
            raise HTTPException(
                status_code=400,
                detail="Invalid report type. Must be 'payment' or 'mtr'"
            )

        if report_type == 'payment' and not file.filename.endswith('.csv'):
            raise HTTPException(
                status_code=400,
                detail="Payment report must be a CSV file"
            )

        if report_type == 'mtr' and not file.filename.endswith('.xlsx'):
            raise HTTPException(
                status_code=400,
                detail="MTR report must be an Excel file"
            )
            
        # Validate file size (max 10MB)
        max_size = 100 * 1024 * 1024  # 10MB in bytes
        contents = await file.read()
        if len(contents) > max_size:
            raise HTTPException(
                status_code=400,
                detail="File too large. Maximum size is 10MB."
            )
            
        # Log file upload
        logger.info(f"Received {report_type} report: {file.filename} ({len(contents)} bytes)")
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Store job details
        processing_jobs[job_id] = {
            "status": "processing",
            "file_name": file.filename,
            "report_type": report_type,
            "file_size": len(contents),
            "start_time": datetime.now().isoformat()
        }

        # Store file content in memory
        if report_type == 'payment':
            processing_jobs[job_id]['payment_content'] = contents
        else:
            processing_jobs[job_id]['mtr_content'] = contents

        # Check if we have both files
        job_with_both_files = None
        for job in processing_jobs.values():
            if 'payment_content' in job and 'mtr_content' in job:
                job_with_both_files = job
                break

        # If we have both files, process them
        if job_with_both_files:
            try:
                # Process the files
                results = process_files(
                    job_with_both_files['mtr_content'],
                    job_with_both_files['payment_content']
                )
                
                # Update job status
                processing_jobs[job_id]["status"] = "completed"
                processing_jobs[job_id]["completion_time"] = datetime.now().isoformat()
                
                logger.info(f"Processing completed for job {job_id}")
                
                return JSONResponse(content={
                    "job_id": job_id,
                    "status": "completed",
                    "message": "Files processed successfully",
                    "data": results
                })
                
            except Exception as e:
                logger.error(f"Error processing files: {str(e)}")
                processing_jobs[job_id]["status"] = "failed"
                processing_jobs[job_id]["error"] = str(e)
                raise HTTPException(status_code=500, detail=f"Error processing files: {str(e)}")
        
        # If we don't have both files yet, return success but indicate we're waiting
        return JSONResponse(content={
            "job_id": job_id,
            "status": "waiting",
            "message": f"Successfully uploaded {report_type} report. Waiting for the other report to process."
        })
        
    except HTTPException as he:
        logger.warning(f"Client error: {str(he.detail)}")
        raise he
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    """Get processing status and results"""
    if job_id not in processing_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JSONResponse(content=processing_jobs[job_id])

if __name__ == "__main__":
    import uvicorn
    # Create required directories
    UPLOAD_DIR.mkdir(exist_ok=True)
    
    uvicorn.run(app, host=API_HOST, port=API_PORT, reload=DEBUG_MODE)
