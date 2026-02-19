import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from glob import glob
import os
import unicodedata

your_path = r"C:\Users\HUGO\Documents\GIS_Estudos\maps\new-ones\inundacao_colection_bh\isoietas\bh-jan-2025-2026\data-year\2026-cvs-files"
# Folder with all CSV files
files = glob(f"{your_path}\inmet_*.csv")
print(f"Found {len(files)} files")

# Function to remove accents
def remove_accents(text):
    if isinstance(text, str):
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text

# List to store data from each station
data = []

for f in files:
    print(f"\n{'='*50}")
    print(f"Processing: {os.path.basename(f)}")
    
    try:
        # ============================================
        # 1. EXTRACT STATION INFORMATION
        # ============================================
        with open(f, 'r', encoding='latin1') as file:
            lines = file.readlines()
        
        station_name = lines[2].split(';')[1].strip()
        lat = float(lines[4].split(';')[1].strip().replace(',', '.'))
        lon = float(lines[5].split(';')[1].strip().replace(',', '.'))
        
        print(f"  Station: {station_name}")
        
        # ============================================
        # 2. FIND WHERE DATA STARTS
        # ============================================
        skiprows = 0
        header_line = None
        for i, line in enumerate(lines):
            if 'PRECIPITAÇÃO' in line.upper() or 'PRECIPITACAO' in line.upper():
                skiprows = i
                header_line = line.strip()
                print(f"  Found header at line {i}")
                break
        
        if header_line is None:
            # Try common pattern
            skiprows = 8
            print(f"  Using default skiprows={skiprows}")
        
        # ============================================
        # 3. READ DATA AND CHECK ACTUAL COLUMNS
        # ============================================
        print(f"\n  Reading data with skiprows={skiprows}...")
        df = pd.read_csv(f, sep=';', encoding='latin1', skiprows=skiprows, decimal=',')
        print(f"  DataFrame shape: {df.shape}")
        
        # Show all columns
        print(f"\n  ALL COLUMNS FOUND:")
        for col in df.columns:
            print(f"    - '{col}'")
        
        # ============================================
        # 4. FIND COLUMNS (WITH TOLERANCE)
        # ============================================
        # Look for date column
        date_col = None
        for col in df.columns:
            col_upper = col.upper()
            if 'DATA' in col_upper:
                date_col = col
                print(f"  ✓ Found date column: '{col}'")
                break
        
        if not date_col:
            print(f"  ⚠️  No date column found, trying first column...")
            date_col = df.columns[0]
        
        # Look for precipitation column
        precip_col = None
        possible_names = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
            'PRECIPITAÇÃO TOTAL, HORARIO (mm)',
            'PRECIPITAÇÃO TOTAL HORÁRIO (mm)',
            'PRECIPITAÇÃO TOTAL HORARIO (mm)',
            'PRECIPITACAO TOTAL, HORARIO (mm)',  # Without accent
            'PRECIPITACAO TOTAL HORARIO (mm)',
            'PRECIPITAÇÃO',
            'PRECIPITACAO',
            'PRECIP.'
        ]
        
        # First, look for exact match
        for possible in possible_names:
            if possible in df.columns:
                precip_col = possible
                print(f"  ✓ Found precipitation column: '{precip_col}'")
                break
        
        # If not found, look for column containing the word
        if precip_col is None:
            for col in df.columns:
                col_normalized = remove_accents(col.upper())
                if 'PRECIP' in col_normalized:
                    precip_col = col
                    print(f"  ✓ Found precipitation column (approximate): '{precip_col}'")
                    break
        
        if not precip_col:
            print(f"  ✗ ERROR: Could not find precipitation column!")
            print(f"  Available columns: {list(df.columns)}")
            continue
        
        # ============================================
        # 5. CONVERT COLUMNS
        # ============================================
        # Convert date
        print(f"\n  First 5 date values:")
        print(f"    {df[date_col].head().tolist()}")
        
        # Try different date formats
        df['DATE_PARSED'] = pd.to_datetime(df[date_col], errors='coerce')
        
        # If failed, try Brazilian formats
        if df['DATE_PARSED'].isna().all():
            try:
                # Try DD/MM/YYYY format
                df['DATE_PARSED'] = pd.to_datetime(df[date_col], format='%d/%m/%Y', errors='coerce')
            except:
                # Try extracting date from string with time
                df['DATE_PARSED'] = pd.to_datetime(df[date_col].astype(str).str.split().str[0], 
                                                  errors='coerce')
        
        print(f"  Parsed dates (first 5):")
        print(f"    {df['DATE_PARSED'].head().dt.strftime('%Y-%m-%d').tolist()}")
        
        # Convert precipitation
        print(f"\n  Precipitation column type: {df[precip_col].dtype}")
        print(f"  First 5 precipitation values:")
        print(f"    {df[precip_col].head().tolist()}")
        
        df[precip_col] = pd.to_numeric(df[precip_col], errors='coerce')
        
        # ============================================
        # 6. CHECK AVAILABLE DATES
        # ============================================
        df['YEAR'] = df['DATE_PARSED'].dt.year
        df['MONTH'] = df['DATE_PARSED'].dt.month
        
        print(f"\n  Date statistics:")
        print(f"    Date range: {df['DATE_PARSED'].min()} to {df['DATE_PARSED'].max()}")
        print(f"    Years: {sorted(df['YEAR'].dropna().unique())}")
        print(f"    Months: {sorted(df['MONTH'].dropna().unique())}")
        
        # Count rows by month/year
        print(f"\n  Data counts by month/year:")
        for year in sorted(df['YEAR'].dropna().unique()):
            for month in sorted(df[df['YEAR'] == year]['MONTH'].dropna().unique()):
                count = len(df[(df['YEAR'] == year) & (df['MONTH'] == month)])
                print(f"    {year}-{month:02d}: {count} rows")
        
        # ============================================
        # 7. FILTER JANUARY
        # ============================================
        jan_2025_mask = (df['YEAR'] == 2025) & (df['MONTH'] == 1)
        df_jan_2025 = df[jan_2025_mask]
        
        if len(df_jan_2025) > 0:
            print(f"\n  ✓ Found January 2025: {len(df_jan_2025)} rows")
            target_df = df_jan_2025
            target_year = 2025
        else:
            print(f"\n  ⚠️  No January 2025 data")
            
            # Look for any January
            jan_any_mask = (df['MONTH'] == 1)
            df_jan_any = df[jan_any_mask]
            
            if len(df_jan_any) > 0:
                available_years = sorted(df_jan_any['YEAR'].dropna().unique())
                most_recent_year = available_years[-1]
                df_jan_recent = df_jan_any[df_jan_any['YEAR'] == most_recent_year]
                
                print(f"  ✓ Using January {most_recent_year}: {len(df_jan_recent)} rows")
                target_df = df_jan_recent
                target_year = most_recent_year
            else:
                print(f"  ✗ No January data in any year!")
                continue
        
        # ============================================
        # 8. CALCULATE PRECIPITATION
        # ============================================
        total_precip_january = target_df[precip_col].sum()
        total_yearly = df[precip_col].sum()
        
        print(f"\n  Precipitation totals:")
        print(f"    January {target_year}: {total_precip_january:.1f} mm")
        print(f"    Full year: {total_yearly:.1f} mm")
        print(f"    Ratio Jan/Year: {(total_precip_january/total_yearly*100):.1f}%")
        
        # ============================================
        # 9. ADD TO LIST
        # ============================================
        data.append({
            'station': station_name,
            'lat': lat,
            'lon': lon,
            'precip_january': total_precip_january,
            'year': target_year,
            'precip_year': total_yearly,
            'geometry': Point(lon, lat)
        })
        
        print(f"\n  ✓ Successfully processed {station_name}")
        
    except Exception as e:
        print(f"  ✗ ERROR: {str(e)[:200]}")
        import traceback
        traceback.print_exc()
        continue

# ============================================
# 10. SAVE RESULTS
# ============================================
if len(data) > 0:
    print(f"\n{'='*60}")
    print(f"PROCESSING COMPLETE!")
    print(f"Total stations processed: {len(data)}")
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(data, geometry='geometry', crs="EPSG:4326")
    
    # Analysis
    print(f"\nSUMMARY BY YEAR:")
    for year in sorted(gdf['year'].unique()):
        year_data = gdf[gdf['year'] == year]
        avg_precip = year_data['precip_january'].mean()
        print(f"  January {year} ({len(year_data)} stations):")
        print(f"    Average: {avg_precip:.1f} mm")
        print(f"    Min: {year_data['precip_january'].min():.1f} mm")
        print(f"    Max: {year_data['precip_january'].max():.1f} mm")
    
    # Save
    save_path = r"C:\Users\HUGO\Desktop\isoeatas\generated"
    output_shp = f"{save_path}\precipitacao_JANEIRO_FINAL_2026.geojson"
    gdf.to_file(output_shp)
    print(f"\n✓ Saved shapefile: {output_shp}")
    
    # Show results
    print(f"\nTOP 10 STATIONS (highest January precipitation):")
    top10 = gdf.nlargest(10, 'precip_january')[['station', 'year', 'precip_january']]
    for idx, row in top10.iterrows():
        print(f"  {row['station']} ({row['year']}): {row['precip_january']:.1f} mm")
    
else:
    print(f"\nERROR: No data processed!")