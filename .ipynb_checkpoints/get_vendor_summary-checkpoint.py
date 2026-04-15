import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import logging

logging.basicConfig(filename = 'logs/get_sales_summary.log',
                    level =  logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s - %(message)s',
                    filemode = 'a')


def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
        WITH FreightSummary AS (
            SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
            FROM vendor_invoice
            GROUP BY VendorNumber
            ),
            
            PurchaseSummary AS (
            SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            avg(p.PurchasePrice) as PurchasePrice,
            avg(pp.Price) AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp
            ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY 
            p.VendorNumber, p.VendorName, p.Brand,pp.Volume
            ),
            
            SalesSummary AS (
            SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand)
            
            SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalExciseTax,
            fs.FreightCost
            FROM PurchaseSummary ps
            LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand
            LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
            ORDER BY ps.TotalPurchaseDollars DESC
            """, conn)
    logging.info('Summary table created succesfully')
    return vendor_sales_summary

def clean_data(df):
        '''This function will clean data'''
        # changing datatype to float
        df['Volume'] = df['Volume'].astype('float')

        #filling missing values with 0
        df.fillna(0,inplace=True)

        # removing spaces from categorical columns
        df['VendorName'] = df['VendorName'].str.strip()

        # creating new columns for better analysis
        df['COGS'] = df['TotalSalesQuantity']*df['PurchasePrice']
        df['GrossProfit'] = df['TotalSalesDollars'] - df['COGS']
        df['ProfitMargin'] = (df['GrossProfit']/df['TotalSalesDollars'])*100
        df['StockTurnover'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
        df['SalestoPurchaseRatio'] = df['TotalSalesDollars']/df['COGS']

        df.replace([np.inf, -np.inf], 0, inplace=True)

        logging.info('Summary table cleaned succesfully')

        return df


if __name__ == '__main__':
    # Asking for database credentials
    db_user = input("Enter your database username: ")
    db_password = input("Enter your database password: ")
    db_name = input("Enter your database name: ")
    db_host = input("Enter your database host (default: localhost): ") or "localhost"

    # Create database connection
    conn = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")
    
    print("Database connection created successfully!")

    summary_df = create_vendor_summary(conn)
    clean_df = clean_data(summary_df)
    clean_df.to_sql(
    'vendor_sales_summary',
    con=conn,
    if_exists='replace',
    index=False)
    logging.info('Summary table save in database succesfully')