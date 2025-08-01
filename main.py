import pandas as pd
import math
from datetime import datetime, timedelta

def round_duration_to_15min(duration_str):
    """
    Round duration up to nearest 15 minutes
    Input format: HH:MM:SS or HH:MM
    Output format: HH:MM:SS
    """
    if pd.isna(duration_str) or duration_str == '':
        return duration_str
    
    # Handle different time formats
    parts = str(duration_str).split(':')
    
    if len(parts) == 2:  # HH:MM format
        hours, minutes = int(parts[0]), int(parts[1])
        seconds = 0
    elif len(parts) == 3:  # HH:MM:SS format
        hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
    else:
        return duration_str  # Return original if format is unexpected
    
    # Convert to total minutes
    total_minutes = hours * 60 + minutes + (seconds / 60)
    
    # Round up to nearest 15 minutes
    rounded_minutes = math.ceil(total_minutes / 15) * 15
    
    # Convert back to hours and minutes
    rounded_hours = rounded_minutes // 60
    remaining_minutes = rounded_minutes % 60
    
    return f"{rounded_hours:02d}:{remaining_minutes:02d}:00"

def duration_to_hours(duration_str):
    """
    Convert duration string to decimal hours
    Input format: HH:MM:SS
    Output: decimal hours (e.g., 7.5 for 7:30:00)
    """
    if pd.isna(duration_str) or duration_str == '':
        return 0.0
    
    parts = str(duration_str).split(':')
    if len(parts) >= 2:
        hours = int(parts[0])
        minutes = int(parts[1])
        return hours + (minutes / 60.0)
    return 0.0

def hours_to_duration(hours):
    """
    Convert decimal hours back to HH:MM:SS format
    """
    total_minutes = int(hours * 60)
    hrs = total_minutes // 60
    mins = total_minutes % 60
    return f"{hrs:02d}:{mins:02d}:00"

def process_timesheet(file_path):
    """
    Process the timesheet CSV file
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Display original data info
        print("Original data shape:", df.shape)
        print("\nColumns:", df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head())
        
        # Delete specified columns
        columns_to_delete = ['user', 'email', 'task', 'billable', 'start time', 'end date', 'end time', 'tags']
        existing_columns_to_delete = []
        
        for col_to_delete in columns_to_delete:
            for col in df.columns:
                if col.lower() == col_to_delete.lower():
                    existing_columns_to_delete.append(col)
                    break
        
        if existing_columns_to_delete:
            print(f"\nDeleting columns: {existing_columns_to_delete}")
            df = df.drop(columns=existing_columns_to_delete)
        
        print(f"\nColumns after deletion: {df.columns.tolist()}")
        
        # Check if required columns exist
        duration_col = None
        client_col = None
        description_col = None
        
        for col in df.columns:
            if col.lower() == 'duration':
                duration_col = col
            elif col.lower() == 'client':
                client_col = col
            elif col.lower() == 'description':
                description_col = col
        
        if duration_col is None:
            print("\nWarning: 'duration' column not found. Available columns:", df.columns.tolist())
            return
        
        if client_col is None:
            print("\nWarning: 'client' column not found. Available columns:", df.columns.tolist())
            return
            
        if description_col is None:
            print("\nWarning: 'description' column not found. Available columns:", df.columns.tolist())
            return
        
        # Round duration values up to nearest 15 minutes
        print(f"\nProcessing {duration_col} column...")
        df[duration_col] = df[duration_col].apply(round_duration_to_15min)
        
        # Store original count for comparison
        original_count = len(df)
        
        # Group by client and description, summing durations for matching descriptions
        print(f"\nGrouping by {client_col} and {description_col} and combining durations...")
        
        # Convert durations to hours for easier summation
        df['temp_hours'] = df[duration_col].apply(duration_to_hours)
        
        # Group by client and description, sum the hours, and keep other columns
        grouped_df = df.groupby([client_col, description_col], as_index=False).agg({
            'temp_hours': 'sum',
            # Keep first occurrence of other columns
            **{col: 'first' for col in df.columns if col not in [client_col, description_col, duration_col, 'temp_hours']}
        })
        
        # Convert hours back to duration format
        grouped_df[duration_col] = grouped_df['temp_hours'].apply(hours_to_duration)
        
        # Create Hours column next to description
        desc_index = grouped_df.columns.get_loc(description_col)
        hours_col = grouped_df['temp_hours'].round(2)
        
        # Insert Hours column after description
        grouped_df.insert(desc_index + 1, 'Hours', hours_col)
        
        # Drop temporary hours column
        grouped_df = grouped_df.drop('temp_hours', axis=1)
        
        # Sort by client name alphabetically
        print(f"\nSorting by {client_col} column...")
        grouped_df = grouped_df.sort_values(by=client_col, ascending=True)
        
        # Reset index after sorting
        grouped_df = grouped_df.reset_index(drop=True)
        
        df = grouped_df
        
        # Save the processed data
        output_file = file_path.replace('.csv', '_processed.csv')
        df.to_csv(output_file, index=False)
        
        print(f"\nProcessed data saved to: {output_file}")
        print("\nProcessed data shape:", df.shape)
        print("\nFirst few rows of processed data:")
        print(df.head())
        
        # Show some examples of duration and hours conversion
        print(f"\nDuration and Hours examples:")
        print("Duration -> Hours")
        sample_data = df[[duration_col, 'Hours']].head(10)
        for _, row in sample_data.iterrows():
            print(f"{row[duration_col]} -> {row['Hours']} hours")
        
        # Show grouping information
        print(f"\nGrouping summary:")
        print(f"Original entries: {len(df) if 'original_count' not in locals() else original_count}")
        print(f"After grouping: {len(df)}")
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        print("Please make sure the file exists in your project directory.")
    except Exception as e:
        print(f"Error processing file: {str(e)}")

# Main execution
if __name__ == "__main__":
    # Process the timesheet
    processed_df = process_timesheet('timesheet.csv')
    
    # Optional: Display client grouping
    if processed_df is not None:
        print("\nClient grouping preview:")
        client_col = None
        for col in processed_df.columns:
            if col.lower() == 'client':
                client_col = col
                break
        
        if client_col:
            client_counts = processed_df[client_col].value_counts()
            print(f"\nNumber of entries per client:")
            for client, count in client_counts.items():
                print(f"  {client}: {count} entries")