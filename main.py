from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

PROJECT_ID = "mgmt545-489015"
DATASET = "property_mgmt"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # accept requests from any origin
    allow_methods=["GET", "POST"],
    allow_headers=["*"],       # accept any request headers
)

class IncomeCreateRequest(BaseModel):
    property_id: int
    amount: float
    date: str
    description: str


class ExpenseCreateRequest(BaseModel):
    property_id: int
    amount: float
    date: str
    category: str
    vendor: str
    description: str

class PropertyCreateRequest(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: str
    monthly_rent: float


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a specific property by its ID.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    prop = [dict(row) for row in results]
    return prop


@app.post("/properties")
def add_property(payload: PropertyCreateRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new property in the database.
    """
    if payload.monthly_rent <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Monthly rent cannot be less than or equal to zero"
        )

    insert_query = f'''
        DECLARE new_property_id INT64;
        SET new_property_id = (
            SELECT MAX(property_id)
            FROM `mgmt545-489015.property_mgmt.properties`
            ) + 1;

        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`(
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        )
        VALUES (
            new_property_id,
            '{payload.name}',
            '{payload.address}',
            '{payload.city}',
            '{payload.state}',
            '{payload.postal_code}',
            '{payload.property_type}',
            '{payload.tenant_name}',
            {payload.monthly_rent}
        )
    '''

    try:
        results = bq.query(insert_query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all income recoreds for a property
    """
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    income = [dict(row) for row in results]
    return income

@app.post("/income")
def add_income(payload: IncomeCreateRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new income record for a property
    """
    
    if payload.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount cannot be less than or equal to zero"
        )    


    insert_query = f'''
        DECLARE new_income_id INT64;
        SET new_income_id = (
            SELECT MAX(income_id)
            FROM `mgmt545-489015.property_mgmt.income`
            ) + 1;

        INSERT INTO `{PROJECT_ID}.{DATASET}.income`(income_id, property_id, amount, date, description)
        VALUES (new_income_id, {payload.property_id}, {payload.amount}, '{payload.date}', '{payload.description}')
    '''

    try:
        results = bq.query(insert_query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    confirm_query = f'''
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE income_id = (
            SELECT MAX(income_id)
            FROM `mgmt545-489015.property_mgmt.income`
            )
    '''

    try:
        results = bq.query(confirm_query).result() 
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    confirmation = [dict(row) for row in results]
    return confirmation

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all expense recoreds for a property
    """

    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    expenses = [dict(row) for row in results]
    return expenses

@app.post("/expenses")
def add_expense(payload: ExpenseCreateRequest, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new expense record for a property
    """
    if payload.amount <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Amount cannot be less than or equal to zero"
        )

    query = f'''
        DECLARE new_expense_id INT64;
        SET new_expense_id = (
            SELECT MAX(expense_id)
            FROM `mgmt545-489015.property_mgmt.expenses`
            ) + 1;

        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`(expense_id, property_id, amount, date, category, vendor, description)
        VALUES (new_expense_id, {payload.property_id}, {payload.amount}, '{payload.date}','{payload.category}', '{payload.vendor}', '{payload.description}')
    '''

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    confirm_query = f'''
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE expense_id = (
            SELECT MAX(expense_id)
            FROM `mgmt545-489015.property_mgmt.expenses`
            )
    '''

    try:
        results = bq.query(confirm_query).result() 
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    confirmation = [dict(row) for row in results]
    return confirmation
