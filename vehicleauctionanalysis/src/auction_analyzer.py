"""
Vehicle Auction Profitability Analyzer
Analyzes auction data to identify best deals, worst deals, and ROI patterns.
"""

import pandas as pd
from datetime import datetime

# Load data
df = pd.read_csv('../data/auction_data.csv')

print("="*80)
print("VEHICLE AUCTION PROFITABILITY ANALYZER")
print("="*80)
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total vehicles analyzed: {len(df)}\n")

# ============================================================================
# OVERALL SUMMARY
# ============================================================================

print("OVERALL SUMMARY")
print("-" * 80)
print(f"Total Bid Investment:     ${df['bid_amount'].sum():,.2f}")
print(f"Total Market Value:       ${df['market_value'].sum():,.2f}")
print(f"Total Costs:              ${df['total_cost'].sum():,.2f}")
print(f"Total Profit:             ${df['profit'].sum():,.2f}")
print(f"Average ROI:              {df['roi_percent'].mean():.2f}%")
print(f"Best ROI:                 {df['roi_percent'].max():.2f}%")
print(f"Worst ROI:                {df['roi_percent'].min():.2f}%")
print()

# ============================================================================
# BEST DEALS (ROI > 15%)
# ============================================================================

print("BEST DEALS (ROI > 15%)")
print("-" * 80)
best_deals = df[df['roi_percent'] > 15].sort_values('roi_percent', ascending=False)
print(f"Found {len(best_deals)} high-ROI deals:\n")

for idx, row in best_deals.head(10).iterrows():
    print(f"  #{row['auction_id']:2d} | {row['vehicle_type']:20s} ({row['condition']:10s}) | "
          f"Bid: ${row['bid_amount']:>9,.0f} | ROI: {row['roi_percent']:>6.2f}% | "
          f"Profit: ${row['profit']:>8,.0f}")

print()

# ============================================================================
# WORST DEALS (ROI < 0% or Close to Break-Even)
# ============================================================================

print("WORST DEALS (ROI < 0%)")
print("-" * 80)
worst_deals = df[df['roi_percent'] < 0].sort_values('roi_percent')
print(f"Found {len(worst_deals)} money-losing deals:\n")

if len(worst_deals) > 0:
    for idx, row in worst_deals.head(5).iterrows():
        print(f"  #{row['auction_id']:2d} | {row['vehicle_type']:20s} ({row['condition']:10s}) | "
              f"Bid: ${row['bid_amount']:>9,.0f} | ROI: {row['roi_percent']:>6.2f}% | "
              f"Loss: ${row['profit']:>8,.0f}")
else:
    print("  No money-losing deals found!")

print()

# ============================================================================
# ANALYSIS BY VEHICLE TYPE
# ============================================================================

print("ANALYSIS BY VEHICLE TYPE")
print("-" * 80)
by_type = df.groupby('vehicle_type').agg({
    'auction_id': 'count',
    'roi_percent': ['mean', 'max', 'min'],
    'profit': 'sum'
}).round(2)

by_type.columns = ['Count', 'Avg ROI %', 'Max ROI %', 'Min ROI %', 'Total Profit']
by_type = by_type.sort_values('Avg ROI %', ascending=False)

print(f"{'Vehicle Type':<20} {'Count':<8} {'Avg ROI %':<12} {'Max ROI %':<12} {'Total Profit'}")
print("-" * 70)
for vehicle, row in by_type.iterrows():
    print(f"{vehicle:<20} {int(row['Count']):<8} {row['Avg ROI %']:>10.2f}% "
          f"{row['Max ROI %']:>10.2f}% ${row['Total Profit']:>11,.0f}")

print()

# ============================================================================
# ANALYSIS BY CONDITION
# ============================================================================

print("ANALYSIS BY CONDITION")
print("-" * 80)
by_condition = df.groupby('condition').agg({
    'auction_id': 'count',
    'roi_percent': ['mean', 'max', 'min'],
    'recon_cost': 'mean',
    'profit': 'sum'
}).round(2)

by_condition.columns = ['Count', 'Avg ROI %', 'Max ROI %', 'Min ROI %', 'Avg Recon Cost', 'Total Profit']
by_condition = by_condition.sort_values('Avg ROI %', ascending=False)

print(f"{'Condition':<15} {'Count':<8} {'Avg ROI %':<12} {'Avg Recon':<12} {'Total Profit'}")
print("-" * 60)
for condition, row in by_condition.iterrows():
    print(f"{condition:<15} {int(row['Count']):<8} {row['Avg ROI %']:>10.2f}% "
          f"${row['Avg Recon Cost']:>9,.0f} ${row['Total Profit']:>11,.0f}")

print()

# ============================================================================
# TESLA MODEL Y DEEP DIVE
# ============================================================================

print("TESLA MODEL Y ANALYSIS (High-Value Opportunity)")
print("-" * 80)
teslas = df[df['vehicle_type'] == 'Tesla Model Y']
if len(teslas) > 0:
    print(f"Total Tesla Model Y vehicles: {len(teslas)}")
    print(f"Average bid:        ${teslas['bid_amount'].mean():,.2f}")
    print(f"Average market value: ${teslas['market_value'].mean():,.2f}")
    print(f"Average ROI:        {teslas['roi_percent'].mean():.2f}%")
    print(f"Total potential profit: ${teslas['profit'].sum():,.2f}\n")
    
    print("Tesla deals by condition:")
    for condition in ['Excellent', 'Good', 'Fair', 'Poor']:
        condition_teslas = teslas[teslas['condition'] == condition]
        if len(condition_teslas) > 0:
            print(f"  {condition:<10}: {len(condition_teslas)} vehicles, "
                  f"Avg ROI: {condition_teslas['roi_percent'].mean():.2f}%, "
                  f"Total Profit: ${condition_teslas['profit'].sum():,.0f}")
else:
    print("No Tesla Model Y vehicles in dataset")

print()

# ============================================================================
# EXPORT RESULTS
# ============================================================================

print("EXPORTING RESULTS")
print("-" * 80)

# Export 1: Best deals
best_deals.to_csv('../output/01_best_deals.csv', index=False)
print("✓ Exported: Best Deals (ROI > 15%)")

# Export 2: All vehicles sorted by ROI
df_sorted = df.sort_values('roi_percent', ascending=False)
df_sorted.to_csv('../output/02_all_vehicles_by_roi.csv', index=False)
print("✓ Exported: All Vehicles by ROI")

# Export 3: Summary by vehicle type
by_type.to_csv('../output/03_summary_by_vehicle_type.csv')
print("✓ Exported: Summary by Vehicle Type")

# Export 4: Summary by condition
by_condition.to_csv('../output/04_summary_by_condition.csv')
print("✓ Exported: Summary by Condition")

print()
print("="*80)
print("✓ Analysis Complete")
print("="*80)