import json
import pandas as pd
from datetime import datetime
import os
from typing import List, Dict, Optional

def parse_walmart_order(json_file_path):
    """
    解析Walmart订单JSON数据,将订单头和订单行组合成DataFrame
    
    参数:
        json_file_path: JSON文件路径
        
    返回:
        包含订单头和订单行组合数据的DataFrame
    """
    # 读取JSON文件
    with open(json_file_path, 'r', encoding='utf-8') as f:
        orders_data = json.load(f)
    
    # 存储解析后的数据
    parsed_data = []
    
    # 遍历每个订单
    for order_idx, order in enumerate(orders_data):
        try:
            # 提取订单头信息
            ship_node = order.get('shipNode', {})
            order_header = {
                'purchaseOrderId': order.get('purchaseOrderId'),
                'customerOrderId': order.get('customerOrderId'),
                'customerEmailId': order.get('customerEmailId'),
                'orderDate': order.get('orderDate'),
                'orderDate_formatted': datetime.fromtimestamp(order.get('orderDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if order.get('orderDate') else None,
                'shipNode_type': ship_node.get('type') if ship_node else None,
                'shipNode_name': ship_node.get('name') if ship_node else None,
                'shipNode_id': ship_node.get('id') if ship_node else None,
                'source_file': os.path.basename(json_file_path)  # 添加源文件信息
            }
            
            # 提取配送信息
            shipping_info = order.get('shippingInfo', {})
            shipping_data = {
                'phone': shipping_info.get('phone'),
                'estimatedDeliveryDate': shipping_info.get('estimatedDeliveryDate'),
                'estimatedDeliveryDate_formatted': datetime.fromtimestamp(shipping_info.get('estimatedDeliveryDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if shipping_info.get('estimatedDeliveryDate') else None,
                'estimatedShipDate': shipping_info.get('estimatedShipDate'),
                'estimatedShipDate_formatted': datetime.fromtimestamp(shipping_info.get('estimatedShipDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if shipping_info.get('estimatedShipDate') else None,
                'methodCode': shipping_info.get('methodCode')
            }
            
            # 提取地址信息
            postal_address = shipping_info.get('postalAddress', {})
            address_data = {
                'recipient_name': postal_address.get('name'),
                'address1': postal_address.get('address1'),
                'address2': postal_address.get('address2'),
                'city': postal_address.get('city'),
                'state': postal_address.get('state'),
                'postalCode': postal_address.get('postalCode'),
                'country': postal_address.get('country'),
                'addressType': postal_address.get('addressType')
            }
            
            # 合并订单头和配送信息
            header_data = {**order_header, **shipping_data, **address_data}
            
            # 获取订单行
            order_lines_container = order.get('orderLines', {})
            if order_lines_container is None:
                continue
                
            order_lines = order_lines_container.get('orderLine', [])
            
            # 如果orderLines为None，跳过这个订单
            if order_lines is None:
                continue
                
            # 如果只有一个订单行，确保它是列表形式
            if isinstance(order_lines, dict):
                order_lines = [order_lines]
            
            # 为每个订单行创建一行数据
            for line_idx, line in enumerate(order_lines):
                try:
                    # 提取订单行信息
                    item = line.get('item', {}) if line else {}
                    line_data = {
                        'lineNumber': line.get('lineNumber') if line else None,
                        'sku': item.get('sku') if item else None,
                        'productName': item.get('productName') if item else None,
                        'condition': item.get('condition') if item else None,
                        'quantity': line.get('orderLineQuantity', {}).get('amount') if line else None,
                        'unitOfMeasurement': line.get('orderLineQuantity', {}).get('unitOfMeasurement') if line else None,
                        'statusDate': line.get('statusDate') if line else None,
                        'statusDate_formatted': datetime.fromtimestamp(line.get('statusDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if line and line.get('statusDate') else None,
                        'fulfillmentOption': line.get('fulfillment', {}).get('fulfillmentOption') if line else None,
                        'shipMethod': line.get('fulfillment', {}).get('shipMethod') if line else None,
                        'storeId': line.get('fulfillment', {}).get('storeId') if line else None,
                        'shippingProgramType': line.get('fulfillment', {}).get('shippingProgramType') if line else None
                    }
                    
                    # 提取价格信息
                    charges_container = line.get('charges', {}) if line else {}
                    if charges_container:
                        charges = charges_container.get('charge', [])
                        if charges and len(charges) > 0:
                            charge = charges[0]  # 取第一个charge
                            charge_amount = charge.get('chargeAmount', {}) if charge else {}
                            tax = charge.get('tax', {}) if charge else {}
                            price_data = {
                                'chargeType': charge.get('chargeType') if charge else None,
                                'chargeName': charge.get('chargeName') if charge else None,
                                'chargeAmount': charge_amount.get('amount') if charge_amount else None,
                                'currency': charge_amount.get('currency') if charge_amount else None,
                                'taxAmount': tax.get('taxAmount', {}).get('amount') if tax else None,
                                'taxName': tax.get('taxName') if tax else None
                            }
                            line_data.update(price_data)
                    
                    # 提取订单状态信息
                    order_line_statuses_container = line.get('orderLineStatuses', {}) if line else {}
                    if order_line_statuses_container:
                        order_line_statuses = order_line_statuses_container.get('orderLineStatus', [])
                        if order_line_statuses and len(order_line_statuses) > 0:
                            status = order_line_statuses[0]  # 取第一个状态
                            status_data = {
                                'orderLineStatus': status.get('status') if status else None,
                                'statusQuantity': status.get('statusQuantity', {}).get('amount') if status else None,
                                'cancellationReason': status.get('cancellationReason') if status else None
                            }
                            
                            # 提取跟踪信息
                            tracking_info = status.get('trackingInfo', {}) if status else {}
                            if tracking_info:
                                carrier_name = tracking_info.get('carrierName', {}) if tracking_info else {}
                                tracking_data = {
                                    'shipDateTime': tracking_info.get('shipDateTime') if tracking_info else None,
                                    'shipDateTime_formatted': datetime.fromtimestamp(tracking_info.get('shipDateTime', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if tracking_info and tracking_info.get('shipDateTime') else None,
                                    'carrierName': carrier_name.get('carrier') or carrier_name.get('otherCarrier') if carrier_name else None,
                                    'methodCode': tracking_info.get('methodCode') if tracking_info else None,
                                    'carrierMethodCode': tracking_info.get('carrierMethodCode') if tracking_info else None,
                                    'trackingNumber': tracking_info.get('trackingNumber') if tracking_info else None,
                                    'trackingURL': tracking_info.get('trackingURL') if tracking_info else None
                                }
                                status_data.update(tracking_data)
                            
                            line_data.update(status_data)
                    
                    # 合并订单头和订单行数据
                    combined_data = {**header_data, **line_data}
                    parsed_data.append(combined_data)
                except Exception as e:
                    print(f"处理订单 {order_idx} 的第 {line_idx} 行时出错: {str(e)}")
                    continue
        except Exception as e:
            print(f"处理订单 {order_idx} 时出错: {str(e)}")
            continue
    
    # 创建DataFrame
    df = pd.DataFrame(parsed_data)
    
    return df

def parse_multiple_walmart_orders(json_file_paths):
    """
    解析多个Walmart订单JSON文件，将结果合并到一个DataFrame中
    
    参数:
        json_file_paths: JSON文件路径列表
        
    返回:
        包含所有订单数据的合并DataFrame
    """
    all_dataframes = []
    
    for file_path in json_file_paths:
        try:
            df = parse_walmart_order(file_path)
            print(f"成功解析文件 {file_path}，共 {len(df)} 行数据")
            all_dataframes.append(df)
        except Exception as e:
            print(f"解析文件 {file_path} 时出错: {str(e)}")
    
    if not all_dataframes:
        print("没有成功解析任何文件")
        return pd.DataFrame()
    
    # 合并所有DataFrame
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"所有文件合并完成，共 {len(merged_df)} 行数据")
    
    return merged_df


def parse_walmart_order_json_string(json_str: str, source_file: Optional[str] = None) -> List[Dict]:
    """
    Parse Walmart order JSON string for Flink streaming processing
    
    Args:
        json_str: JSON string of a single order or list of orders
        source_file: Optional source file name for tracking
        
    Returns:
        List of parsed order dictionaries (one dict per order line)
        
    Note:
        This function is designed for Flink streaming processing.
        It parses a single order JSON string and returns a list of dictionaries,
        where each dictionary represents one order line item.
    """
    try:
        # Parse JSON string
        order_data = json.loads(json_str)
        
        # Handle both single order dict and list of orders
        if isinstance(order_data, list):
            orders = order_data
        else:
            orders = [order_data]
        
        parsed_data = []
        
        # Process each order
        for order in orders:
            try:
                # Extract order header information
                ship_node = order.get('shipNode', {})
                order_header = {
                    'purchaseOrderId': order.get('purchaseOrderId'),
                    'customerOrderId': order.get('customerOrderId'),
                    'customerEmailId': order.get('customerEmailId'),
                    'orderDate': order.get('orderDate'),
                    'orderDate_formatted': datetime.fromtimestamp(order.get('orderDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if order.get('orderDate') else None,
                    'shipNode_type': ship_node.get('type') if ship_node else None,
                    'shipNode_name': ship_node.get('name') if ship_node else None,
                    'shipNode_id': ship_node.get('id') if ship_node else None,
                    'source_file': source_file or 'kafka_stream'
                }
                
                # Extract shipping information
                shipping_info = order.get('shippingInfo', {})
                shipping_data = {
                    'phone': shipping_info.get('phone'),
                    'estimatedDeliveryDate': shipping_info.get('estimatedDeliveryDate'),
                    'estimatedDeliveryDate_formatted': datetime.fromtimestamp(shipping_info.get('estimatedDeliveryDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if shipping_info.get('estimatedDeliveryDate') else None,
                    'estimatedShipDate': shipping_info.get('estimatedShipDate'),
                    'estimatedShipDate_formatted': datetime.fromtimestamp(shipping_info.get('estimatedShipDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if shipping_info.get('estimatedShipDate') else None,
                    'methodCode': shipping_info.get('methodCode')
                }
                
                # Extract address information
                postal_address = shipping_info.get('postalAddress', {})
                address_data = {
                    'recipient_name': postal_address.get('name'),
                    'address1': postal_address.get('address1'),
                    'address2': postal_address.get('address2'),
                    'city': postal_address.get('city'),
                    'state': postal_address.get('state'),
                    'postalCode': postal_address.get('postalCode'),
                    'country': postal_address.get('country'),
                    'addressType': postal_address.get('addressType')
                }
                
                # Merge order header and shipping information
                header_data = {**order_header, **shipping_data, **address_data}
                
                # Get order lines
                order_lines_container = order.get('orderLines', {})
                if order_lines_container is None:
                    continue
                    
                order_lines = order_lines_container.get('orderLine', [])
                
                # Skip if orderLines is None
                if order_lines is None:
                    continue
                
                # Ensure order_lines is a list
                if isinstance(order_lines, dict):
                    order_lines = [order_lines]
                
                # Create one row per order line
                for line in order_lines:
                    try:
                        # Extract order line information
                        item = line.get('item', {}) if line else {}
                        line_data = {
                            'lineNumber': line.get('lineNumber') if line else None,
                            'sku': item.get('sku') if item else None,
                            'productName': item.get('productName') if item else None,
                            'product_condition': item.get('condition') if item else None,
                            'quantity': line.get('orderLineQuantity', {}).get('amount') if line else None,
                            'unitOfMeasurement': line.get('orderLineQuantity', {}).get('unitOfMeasurement') if line else None,
                            'statusDate': line.get('statusDate') if line else None,
                            'statusDate_formatted': datetime.fromtimestamp(line.get('statusDate', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if line and line.get('statusDate') else None,
                            'fulfillmentOption': line.get('fulfillment', {}).get('fulfillmentOption') if line else None,
                            'shipMethod': line.get('fulfillment', {}).get('shipMethod') if line else None,
                            'storeId': line.get('fulfillment', {}).get('storeId') if line else None,
                            'shippingProgramType': line.get('fulfillment', {}).get('shippingProgramType') if line else None
                        }
                        
                        # Extract charge information
                        charges_container = line.get('charges', {}) if line else {}
                        if charges_container:
                            charges = charges_container.get('charge', [])
                            if charges and len(charges) > 0:
                                charge = charges[0]
                                charge_amount = charge.get('chargeAmount', {}) if charge else {}
                                tax = charge.get('tax', {}) if charge else {}
                                price_data = {
                                    'chargeType': charge.get('chargeType') if charge else None,
                                    'chargeName': charge.get('chargeName') if charge else None,
                                    'chargeAmount': charge_amount.get('amount') if charge_amount else None,
                                    'currency': charge_amount.get('currency') if charge_amount else None,
                                    'taxAmount': tax.get('taxAmount', {}).get('amount') if tax else None,
                                    'taxName': tax.get('taxName') if tax else None
                                }
                                line_data.update(price_data)
                        
                        # Extract order status information
                        order_line_statuses_container = line.get('orderLineStatuses', {}) if line else {}
                        if order_line_statuses_container:
                            order_line_statuses = order_line_statuses_container.get('orderLineStatus', [])
                            if order_line_statuses and len(order_line_statuses) > 0:
                                status = order_line_statuses[0]
                                status_data = {
                                    'orderLineStatus': status.get('status') if status else None,
                                    'statusQuantity': status.get('statusQuantity', {}).get('amount') if status else None,
                                    'cancellationReason': status.get('cancellationReason') if status else None
                                }
                                
                                # Extract tracking information
                                tracking_info = status.get('trackingInfo', {}) if status else {}
                                if tracking_info:
                                    carrier_name = tracking_info.get('carrierName', {}) if tracking_info else {}
                                    tracking_data = {
                                        'shipDateTime': tracking_info.get('shipDateTime') if tracking_info else None,
                                        'shipDateTime_formatted': datetime.fromtimestamp(tracking_info.get('shipDateTime', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S') if tracking_info and tracking_info.get('shipDateTime') else None,
                                        'carrierName': carrier_name.get('carrier') or carrier_name.get('otherCarrier') if carrier_name else None,
                                        'carrierMethodCode': tracking_info.get('carrierMethodCode') if tracking_info else None,
                                        'trackingNumber': tracking_info.get('trackingNumber') if tracking_info else None,
                                        'trackingURL': tracking_info.get('trackingURL') if tracking_info else None
                                    }
                                    status_data.update(tracking_data)
                                
                                line_data.update(status_data)
                        
                        # Merge order header and order line data
                        combined_data = {**header_data, **line_data}
                        parsed_data.append(combined_data)
                    except Exception as e:
                        # Log error but continue processing
                        print(f"Error processing order line: {str(e)}")
                        continue
            except Exception as e:
                # Log error but continue processing
                print(f"Error processing order: {str(e)}")
                continue
        
        return parsed_data
        
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {str(e)}")
        return []
    except Exception as e:
        print(f"Error parsing order JSON: {str(e)}")
        return []


if __name__ == "__main__":
    # Set JSON file paths
    json_file_paths = [
        "/Users/hzz/KMS/flink-learning-practise/flink_project/flink_order_real_time/stage1_basic_etl/walmart_order/walmart_order_2025-10-01.json",
        "/Users/hzz/KMS/flink-learning-practise/flink_project/flink_order_real_time/stage1_basic_etl/walmart_order/walmart_order_2025-10-02.json",
        "/Users/hzz/KMS/flink-learning-practise/flink_project/flink_order_real_time/stage1_basic_etl/walmart_order/walmart_order_2025-10-03.json"
    ]
    
    # Parse order data
    try:
        order_df = parse_multiple_walmart_orders(json_file_paths)
        
        # Display DataFrame information
        print(f"\nParsing completed, total {len(order_df)} rows of data")
        print("\nDataFrame columns:")
        print(order_df.columns.tolist())
        
        # Display first 5 rows
        print("\nFirst 5 rows:")
        print(order_df.head())
        
        # Group by source file and count
        if 'source_file' in order_df.columns:
            print("\nData volume by source file:")
            print(order_df['source_file'].value_counts())
        
        # Save to CSV file (optional)
        output_dir = os.path.dirname(json_file_paths[0])
        output_csv = os.path.join(output_dir, "walmart_orders_combined_parsed.csv")
        order_df.to_csv(output_csv, index=False, encoding='utf-8')
        print(f"\nData saved to: {output_csv}")
        
    except Exception as e:
        print(f"Error occurred during parsing: {str(e)}")