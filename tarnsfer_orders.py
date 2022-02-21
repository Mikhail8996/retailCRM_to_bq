from xml.dom import NotFoundErr
import requests
import json
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class Data_carrier():
    def __init__(self) -> None:

        with open("credintials.json") as c:
            credintials = json.loads(c.read())

    
        self.main_url = credintials['CRM_URL']
        self.crm_api_key = credintials['API_KEY']
        
        self.bq_client = bigquery.Client()
        self.bq_dataset = credintials['dataset']

        
        

    # Извлекает данные по заказам из retailCRM
    def extract_order_data(self):
        orders_url = self.main_url + "orders"
        headers = {"apiKey": self.crm_api_key}
        response = requests.get(orders_url, headers).json()
        if response['success']: 

            #Собираем мета информацию
            meta = response['pagination']
            page = meta['currentPage']
            all_pages = meta['totalPageCount']

            #Собираем первую страницу заказов
            self.orders = self.extract_orders(response['orders'])
            self.product = self.extract_order_items(response['orders'])

            #Собираем остальную информацию
            while page <= all_pages:
                page += 1
                headers['page'] = page
                response = requests.get(orders_url, headers).json()
                if response['success']:
                    self.orders += self.extract_orders(response['orders'])
                    self.product += self.extract_order_items(response['orders'])

    #Извлекает информацию о заказах
    def extract_orders(self, order_list):
        order_table = []
        for order in order_list:
            order_frame = {}
            order_frame['id'] = int(order['id'])
            order_frame['externalId'] = int(order['externalId'])
            order_frame['orderType'] = str(order['orderType'])
            order_frame['orderMethod'] = str(order['orderMethod'])
            order_frame['createdAt'] = str(order['createdAt'])
            order_frame['summ'] = float(order['summ'])
            order_frame['totalSumm'] = float(order['totalSumm'])
            order_frame['prepaySum'] = float(order['prepaySum'])
            order_frame['purchaseSumm'] = float(order['purchaseSumm'])
            order_frame['managerId'] = int(order['managerId'])
            order_frame['customerId'] = int(order['customer']['id'])
            order_frame['contragent'] = str(order['contragent']['contragentType'])
            order_frame['deliveryCost'] = str(order['delivery']['cost'])
            order_table.append(order_frame)

        return order_table

    #Извлекает информацию о продуктах заказа
    def extract_order_items(self, order_list):
        product_table =[]
        for order in order_list:
            for product in order['items']:
                product_frame = {}
                if order['id']==58:
                    test=0
                product_frame['orderId'] = order['id']
                product_frame['id'] = product['offer']['id']
                product_frame['productName'] = product['offer']['name']
                product_frame['discountTotal'] = product['discountTotal']
                product_frame['initialPrice'] = product['initialPrice']
                product_frame['quantity'] = product['quantity']
                product_frame['vatRate'] = product['vatRate']
                product_frame['purchasePrice'] = product['purchasePrice']
                product_table.append(product_frame)

        return product_table

    #Загружает заказы в bq
    def load_to_bq(self):
        bq_dataset = self.bq_client.dataset(self.bq_dataset)
        bq_orders = bq_dataset.table('orders')
        bq_products = bq_dataset.table('products')
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )

        try:
            order_table = self.bq_client.get_table(bq_orders)
            df = pd.DataFrame.from_dict(self.orders)
            df['createdAt']= pd.to_datetime(df['createdAt'])
            load_orders = self.bq_client.load_table_from_dataframe(df, order_table, job_config=job_config)
            load_orders.result()
        except Exception as error:
            print(error)

        try:
            product_table = self.bq_client.get_table(bq_products)
            df = pd.DataFrame.from_dict(self.product)
            load_orders = self.bq_client.load_table_from_dataframe(df, product_table, job_config=job_config)
            load_orders.result()
        except Exception as error:
            print(error)



if __name__ == "__main__":
    loader = Data_carrier()
    loader.extract_order_data()
    loader.load_to_bq()
    

