from aggregator.data_aggregator import DataAggregator

def test_aggregated_data():
    messages = [
        { 'StockCode': '1', 'Quantity': 2,  'Price': 10, 'InvoiceDate': 1711013520.0 },
        { 'StockCode': '2', 'Quantity': 1,  'Price': 20, 'InvoiceDate': 1711013580.0 },
        { 'StockCode': '1', 'Quantity': 2,  'Price': 10, 'InvoiceDate': 1711013820.0 },
        { 'StockCode': '2', 'Quantity': 1,  'Price': 20, 'InvoiceDate': 1711014300.0 },
        { 'StockCode': '2', 'Quantity': 3,  'Price': 20, 'InvoiceDate': 1711014360.0 },
        { 'StockCode': '1', 'Quantity': 2,  'Price': 10, 'InvoiceDate': 1711014420.0 },
        { 'StockCode': '2', 'Quantity': 1,  'Price': 20, 'InvoiceDate': 1711014480.0 },
        { 'StockCode': '1', 'Quantity': 3,  'Price': 10, 'InvoiceDate': 1711014540.0 },
        { 'StockCode': '2', 'Quantity': 1,  'Price': 20, 'InvoiceDate': 1711014600.0 },
        { 'StockCode': '2', 'Quantity': 3,  'Price': 20, 'InvoiceDate': 1711014660.0 }
    ]
    expected_result = [
        { 'StockCode': '1', 'Epoch': 1711013520.0, '10minutesSum': 20 },
        { 'StockCode': '2', 'Epoch': 1711013580.0, '10minutesSum': 20 },
        { 'StockCode': '1', 'Epoch': 1711013820.0, '10minutesSum': 40 },
        { 'StockCode': '2', 'Epoch': 1711014300.0, '10minutesSum': 20 },
        { 'StockCode': '2', 'Epoch': 1711014360.0, '10minutesSum': 80 },
        { 'StockCode': '1', 'Epoch': 1711014420.0, '10minutesSum': 40 },
        { 'StockCode': '2', 'Epoch': 1711014480.0, '10minutesSum': 100 },
        { 'StockCode': '1', 'Epoch': 1711014540.0, '10minutesSum': 50 },
        { 'StockCode': '2', 'Epoch': 1711014600.0, '10minutesSum': 120 },
        { 'StockCode': '2', 'Epoch': 1711014660.0, '10minutesSum': 180 }
    ]

    aggregator = DataAggregator(messages)
    assert aggregator.aggregated_data() == expected_result

