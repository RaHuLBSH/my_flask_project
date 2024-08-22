from flask import Flask, jsonify, request
from pymongo import MongoClient
from flask_cors import CORS
import requests
import redis
import json

app = Flask(__name__)
CORS(app)
client = MongoClient("mongodb+srv://db_user_read:LdmrVA5EDEv4z3Wr@cluster0.n10ox.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client.RQ_Analytics
cache = redis.Redis(host='localhost', port=6379, db=0)

@app.route('/sales_over_time', methods=['GET'])
def sales_over_time():
    interval = request.args.get('interval', 'daily')  # Default to daily
    growth = request.args.get('growth', False)

    # Define the MongoDB group stage based on the interval
    if interval == 'daily':
        format = '%Y-%m-%d'
    elif interval == 'monthly':
        format = '%Y-%m'
    elif interval == 'quarterly':
        format = None  # We handle this separately
    elif interval == 'yearly':
        format = '%Y'
    else:
        return jsonify({"error": "Invalid interval"}), 400

    # Initial pipeline for all intervals
    pipeline = [
        {
            '$addFields': {
                'created_at_date': {
                    '$dateFromString': {
                        'dateString': '$created_at',
                        'format': '%Y-%m-%dT%H:%M:%S%z'
                    },
                },
                'total_price': {
                    '$toDouble': '$total_price_set.shop_money.amount'  # Ensure amount is treated as a number
                }
            }
        }
    ]
    
    # Handle grouping based on the interval
    if interval == 'quarterly':
        pipeline.extend([
            {
                '$addFields': {
                    'year': {'$year': '$created_at_date'},
                    'quarter': {'$ceil': {'$divide': [{'$month': '$created_at_date'}, 3]}}
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': '$year',
                        'quarter': '$quarter'
                    },
                    'totalSales': {'$sum': '$total_price'}
                }
            },
            {
                '$addFields': {
                    '_id': {
                        '$concat': [
                            {'$toString': '$_id.year'},
                            '-Q',
                            {'$toString': '$_id.quarter'}
                        ]
                    }
                }
            }
        ])
    else:
        pipeline.extend([
            {
                '$group': {
                    '_id': {'$dateToString': {'format': format, 'date': '$created_at_date'}},
                    'totalSales': {'$sum': '$total_price'}
                }
            }
        ])

    pipeline.append({
        '$sort': {
            '_id': 1  # Sort by date in ascending order
        }
    })

    # Run the aggregation
    sales = db.shopifyOrders.aggregate(pipeline)
    
    if growth:
        sales_list = list(sales)
        growth_rate = []
        for i in range(1, len(sales_list)):
            previous = sales_list[i-1]['totalSales']
            current = sales_list[i]['totalSales']
            growth = ((current - previous) / previous) * 100 if previous != 0 else 0
            growth_rate.append({'date': sales_list[i]['_id'], 'growthRate': growth})
        return jsonify(growth_rate)
    else:
        return jsonify(list(sales))






@app.route('/new_customers', methods=['GET'])
def new_customers_over_time():
    interval = request.args.get('interval', 'daily')  # Default to daily
    
    # Set the date format based on the interval
    if interval == 'daily':
        format = '%Y-%m-%d'
    elif interval == 'monthly':
        format = '%Y-%m'
    elif interval == 'quarterly':
        format = None  # We'll handle quarterly separately
    elif interval == 'yearly':
        format = '%Y'
    else:
        return jsonify({"error": "Invalid interval"}), 400

    # Base pipeline that is common across all intervals
    pipeline = [
        {
            '$addFields': {
                'created_at_date': {
                    '$dateFromString': {
                        'dateString': '$created_at',
                        'format': '%Y-%m-%dT%H:%M:%S%z'
                    },
                },
            }
        }
    ]

    if interval == 'quarterly':
        # Add quarterly-specific fields and group by year and quarter
        pipeline.extend([
            {
                '$addFields': {
                    'year': {'$year': '$created_at_date'},
                    'quarter': {'$ceil': {'$divide': [{'$month': '$created_at_date'}, 3]}}
                }
            },
            {
                '$group': {
                    '_id': {
                        'year': '$year',
                        'quarter': '$quarter'
                    },
                    'newCustomers': {'$sum': 1}
                }
            },
            {
                '$addFields': {
                    '_id': {
                        '$concat': [
                            {'$toString': '$_id.year'},
                            '-Q',
                            {'$toString': '$_id.quarter'}
                        ]
                    }
                }
            }
        ])
    else:
        # Handle daily, monthly, and yearly intervals
        pipeline.extend([
            {
                '$group': {
                    '_id': {'$dateToString': {'format': format, 'date': '$created_at_date'}},
                    'newCustomers': {'$sum': 1}
                }
            }
        ])

    # Sort the results by _id in ascending order
    pipeline.append(
        {
            '$sort': {'_id': 1}
        }
    )
    
    # Execute the pipeline
    new_customers = db.shopifyCustomers.aggregate(pipeline)
    return jsonify(list(new_customers))




@app.route('/repeat_customers', methods=['GET'])
def repeat_customers_over_time():
    interval = request.args.get('interval', 'daily')  # Default to daily
    
    # Set the date format based on the interval
    if interval == 'daily':
        format = '%Y-%m-%d'
    elif interval == 'monthly':
        format = '%Y-%m'
    elif interval == 'quarterly':
        format = None  # Handle quarterly separately
    elif interval == 'yearly':
        format = '%Y'
    else:
        return jsonify({"error": "Invalid interval"}), 400

    # Base pipeline that is common across all intervals
    pipeline = [
        {
            '$addFields': {
                'created_at_date': {
                    '$dateFromString': {
                        'dateString': '$created_at',
                        'format': '%Y-%m-%dT%H:%M:%S%z'
                    },
                },
            }
        }
    ]

    if interval == 'quarterly':
        # Handle quarterly-specific fields and group by year and quarter
        pipeline.extend([
            {
                '$group': {
                    '_id': {
                        'customer_id': '$customer.id',
                        'year': {'$year': '$created_at_date'},
                        'quarter': {'$ceil': {'$divide': [{'$month': '$created_at_date'}, 3]}}
                    },
                    'orderCount': {'$sum': 1}
                }
            },
            {
                '$match': {'orderCount': {'$gt': 1}}  # Filter to include only repeat customers
            },
            {
                '$group': {
                    '_id': {
                        'year': '$_id.year',
                        'quarter': '$_id.quarter'
                    },
                    'repeatCustomers': {'$sum': 1}
                }
            },
            {
                '$addFields': {
                    '_id': {
                        '$concat': [
                            {'$toString': '$_id.year'},
                            '-Q',
                            {'$toString': '$_id.quarter'}
                        ]
                    }
                }
            }
        ])
    else:
        # Handle daily, monthly, and yearly intervals
        pipeline.extend([
            {
                '$group': {
                    '_id': {
                        'date': {'$dateToString': {'format': format, 'date': '$created_at_date'}},
                        'customer_id': '$customer.id'
                    },
                    'orderCount': {'$sum': 1}
                }
            },
            {
                '$match': {'orderCount': {'$gt': 1}}  # Filter to include only repeat customers
            },
            {
                '$group': {
                    '_id': '$_id.date',
                    'repeatCustomers': {'$sum': 1}
                }
            }
        ])

    # Sort the results by _id in ascending order
    pipeline.append(
        {
            '$sort': {'_id': 1}
        }
    )

    # Execute the pipeline
    repeat_customers = db.shopifyOrders.aggregate(pipeline)
    return jsonify(list(repeat_customers))


@app.route('/customer_distribution', methods=['GET'])
def customer_distribution():
    distribution = db.shopifyCustomers.aggregate([
        {
            '$group': {
                '_id': '$default_address.city',
                'count': {'$sum': 1}
            }
        },
        {
            '$project': {
                '_id': 0,
                'city': '$_id',
                'count': 1
            }
        }
    ])
    
    customer_data = list(distribution)
    api_key = '270a4fe6996848c7a8b7979dbe13f248'
    base_url = 'https://api.opencagedata.com/geocode/v1/json'
    
    for customer in customer_data:
        city = customer['city']
        
        # Check if the result is in the cache
        cached_result = cache.get(city)
        if cached_result:
            latitude, longitude = json.loads(cached_result)
            customer['latitude'] = latitude
            customer['longitude'] = longitude
        else:
            response = requests.get(base_url, params={'q': city, 'key': api_key})
            if response.status_code == 402:
                return jsonify({'error': 'Payment required for API access'}), 402
            
            data = response.json()
            if data['results']:
                geometry = data['results'][0]['geometry']
                latitude, longitude = geometry['lat'], geometry['lng']
                customer['latitude'] = latitude
                customer['longitude'] = longitude
                # Store the result in the cache
                cache.set(city, json.dumps((latitude, longitude)))
            else:
                customer['latitude'] = None
                customer['longitude'] = None

    return jsonify(customer_data)


@app.route('/clv_by_cohorts', methods=['GET'])
def clv_by_cohorts():
    clv_cohorts = db.shopifyOrders.aggregate([
        {
            '$addFields': {
                'created_at_date': {
                    '$dateFromString': {
                        'dateString': '$created_at',
                        'format': '%Y-%m-%dT%H:%M:%S%z'
                    },
                },
                'total_price': {
                    '$toDouble': '$total_price_set.shop_money.amount'  # Ensure amount is treated as a number
                }
            },
        },
        {
            '$group': {
                '_id': {
                    'month': {'$dateToString': {'format': '%Y-%m', 'date': '$created_at_date'}},
                    'customer_id': '$customer.id'
                },
                'totalSpent': {'$sum': '$total_price'}
            }
        },
        {
            '$group': {
                '_id': '$_id.month',
                'lifetimeValue': {'$sum': '$totalSpent'}
            }
        },
        {
            '$sort': {'_id': 1}
        }
    ])
    
    return jsonify(list(clv_cohorts))


if __name__ == '__main__':  
   app.run(debug=True)
