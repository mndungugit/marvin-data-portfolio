"""
Vehicle Auction Data Generator
Generates auction vehicle data for profitability analysis.
"""

import pandas as pd
import random
from datetime import datetime, timedelta

# Vehicle types with base market values
vehicles = [
    {'type': 'Tesla Model Y', 'base_value': 45000},
    {'type': 'Honda Accord', 'base_value': 18000},
    {'type': 'Ford F-150', 'base_value': 32000},
    {'type': 'Toyota Camry', 'base_value': 16000},
    {'type': 'BMW 3 Series', 'base_value': 22000},
    {'type': 'Chevy Silverado', 'base_value': 28000},
    {'type': 'Nissan Altima', 'base_value': 14000},
    {'type': 'Hyundai Elantra', 'base_value': 12000},
]

conditions = ['Excellent', 'Good', 'Fair', 'Poor']
condition_multipliers = {
    'Excellent': 1.0,
    'Good': 0.85,
    'Fair': 0.70,
    'Poor': 0.50
}

# Generate 50 auction vehicles
data = []
for i in range(1, 51):
    vehicle = random.choice(vehicles)
    condition = random.choice(conditions)
    
    # Market value = base value * condition multiplier
    market_value = vehicle['base_value'] * condition_multipliers[condition]
    
    # Bid amount (varies from 40% to 80% of market value)
    bid_amount = random.uniform(market_value * 0.4, market_value * 0.8)
    
    # Costs
    ga_sales_tax = bid_amount * 0.065  # Georgia 6.5% sales tax
    auction_fees = bid_amount * 0.06   # 6% auction fees
    transport_cost = random.uniform(200, 500)  # Transport to your location
    recon_cost = random.uniform(300, 1500) if condition in ['Fair', 'Poor'] else random.uniform(100, 500)
    
    # Total cost
    total_cost = bid_amount + ga_sales_tax + auction_fees + transport_cost + recon_cost
    
    # Profit & ROI
    profit = market_value - total_cost
    roi_percent = (profit / total_cost) * 100 if total_cost > 0 else 0
    
    data.append({
        'auction_id': i,
        'vehicle_type': vehicle['type'],
        'condition': condition,
        'market_value': round(market_value, 2),
        'bid_amount': round(bid_amount, 2),
        'ga_sales_tax': round(ga_sales_tax, 2),
        'auction_fees': round(auction_fees, 2),
        'transport_cost': round(transport_cost, 2),
        'recon_cost': round(recon_cost, 2),
        'total_cost': round(total_cost, 2),
        'profit': round(profit, 2),
        'roi_percent': round(roi_percent, 2)
    })

# Create DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv('auction_data.csv', index=False)

print(f"✓ Generated {len(df)} auction vehicles")
print(f"✓ Saved to: auction_data.csv")
print(f"\nSample deals:")
print(df[['auction_id', 'vehicle_type', 'condition', 'bid_amount', 'market_value', 'roi_percent']].head(10))