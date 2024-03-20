class DataAggregator:
    def __init__(self, data: list) -> None:
        self.data = data
        self.ten_mins_in_seconds = 10 * 60

    def _rolling_sum(self, epoch_time: float, stock_code: str):
        return sum(
            mes['Quantity'] * mes['Price']
            for mes in self.data
            if mes['StockCode'] == stock_code and
                (
                    mes['InvoiceDate'] <= epoch_time and
                    mes['InvoiceDate'] >= epoch_time - self.ten_mins_in_seconds
                )
        )

    def aggregated_data(self) -> list:
        aggregate_data = []
        for message in self.data:
            stock_code = message['StockCode']
            epoch_time = message['InvoiceDate']

            aggregate_data.append({
                'StockCode': stock_code,
                'Epoch': epoch_time,
                '10minutesSum': self._rolling_sum(epoch_time, stock_code)
            })

        return aggregate_data
