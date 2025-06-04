import os
import pandas as pd
import platform
import subprocess
import threading
import queue
from datetime import datetime
import numpy as np
import multiprocessing as mp
import tkinter as tk
from tkinter import filedialog
from tqdm import tqdm  # Progress bar

# Global queues
file_queue = queue.Queue()
results_queue = queue.Queue()

def process_csv_file(file_path, result_queue=None):
    """Process the CSV file to extract EOD data and save results after combining all filtered chunks."""
    try:
        chunk_size = 500000
        chunks = pd.read_csv(
            file_path,
            dtype={
                'TIME_FRAME': 'category',
                'CATEGORY': 'category',
                'VOLUME': np.int64
            },
            chunksize=chunk_size
        )
        
        filtered_chunks = []
        
        for chunk in chunks:
            mask = (
                (chunk['TIME_FRAME'] == 'EOD') & 
                (chunk['CATEGORY'] != 'ETF') & 
                (chunk['VOLUME'] > 100000)
            )
            result_chunk = chunk[mask]
            if not result_chunk.empty:
                filtered_chunks.append(result_chunk)

        if filtered_chunks:
            final_df = pd.concat(filtered_chunks, ignore_index=True)
            
            # Save filtered data to output file
            current_date = datetime.now().strftime('%Y%m%d')
            output_dir = os.path.dirname(file_path)
            output_filename = f"OUTPUT_NSE_CM_STOCKS_EOD_{current_date}_{os.path.basename(file_path)}"
            output_path = os.path.join(output_dir, output_filename)
            
            final_df.to_csv(output_path, index=False)

            if result_queue is not None:
                result_queue.put((file_path, output_path))
            return output_path
        else:
            if result_queue is not None:
                result_queue.put((file_path, None))
            return None
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        if result_queue is not None:
            result_queue.put((file_path, None))
        return None

def process_file_worker(file_queue, result_queue, pbar):
    """Worker function for processing files."""
    while True:
        try:
            file_path = file_queue.get(block=False)
            process_csv_file(file_path, result_queue)
            file_queue.task_done()
            pbar.update(1)
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            file_queue.task_done()
            pbar.update(1)

def process_files_with_feedback(file_paths):
    """Process multiple selected files with a thread pool."""
    output_files = []
    
    if not file_paths:
        print("No files selected.")
        return []

    for file_path in file_paths:
        file_queue.put(file_path)

    num_workers = min(os.cpu_count() or 4, 8)
    
    with tqdm(total=len(file_paths), desc="Processing Files") as pbar:
        threads = []
        for _ in range(num_workers):
            t = threading.Thread(
                target=process_file_worker,
                args=(file_queue, results_queue, pbar),
                daemon=True
            )
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()

    while not results_queue.empty():
        src_file, dest_file = results_queue.get_nowait()
        if dest_file:
            output_files.append(dest_file)

    return output_files

def combine_all_outputs_into_master(output_files, master_output_path):
    """Combine all individual output CSVs into one master CSV."""
    if not output_files:
        print("No output files to combine.")
        return None
    
    combined_df_list = []
    for file_path in output_files:
        try:
            df = pd.read_csv(file_path)
            combined_df_list.append(df)
        except Exception as e:
            print(f"Failed to read {file_path}: {str(e)}")
    
    if combined_df_list:
        master_df = pd.concat(combined_df_list, ignore_index=True)
        master_df.to_csv(master_output_path, index=False)
        print(f"✅ Master CSV created successfully: {master_output_path}")
        return master_output_path
    else:
        print("No valid data found to combine into master file.")
        return None

def delete_individual_outputs(output_files):
    """Delete all individual output CSVs after creating master."""
    for file_path in output_files:
        try:
            os.remove(file_path)
            print(f"🗑️ Deleted: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Failed to delete {file_path}: {str(e)}")

def show_results(output_files, master_output_path):
    """Display the output files processed and the master CSV."""
    if not output_files:
        print("No files were successfully processed.")
        return

    print(f"\n✅ Successfully processed {len(output_files)} file(s):")
    for output_file in output_files:
        print(os.path.basename(output_file))
    
    if master_output_path:
        print(f"\n📄 Master CSV file: {os.path.basename(master_output_path)}")

def select_files_or_folder():
    """Let user select either multiple files or a folder."""
    root = tk.Tk()
    root.withdraw()

    choice = input("\nChoose input method:\n1. Select multiple CSV files\n2. Select a folder containing CSV files\nEnter 1 or 2: ").strip()

    if choice == '1':
        file_paths = filedialog.askopenfilenames(
            title="Select one or more CSV files",
            filetypes=[("CSV files", "*.csv")]
        )
        return list(file_paths)
    
    elif choice == '2':
        folder_path = filedialog.askdirectory(
            title="Select a folder containing CSV files"
        )
        if folder_path:
            # List all CSV files inside the selected folder
            csv_files = [
                os.path.join(folder_path, filename)
                for filename in os.listdir(folder_path)
                if filename.lower().endswith('.csv')
            ]
            return csv_files
        else:
            return []
    else:
        print("Invalid input. Exiting...")
        return []

def main():
    if platform.system() == 'Windows':
        mp.set_start_method('spawn', force=True)

    # Let user pick multiple files or folder
    file_paths = select_files_or_folder()

    if not file_paths:
        print("No files selected. Exiting...")
        return

    # Output directory
    output_directory = os.path.dirname(file_paths[0])

    # Process all files
    output_files = process_files_with_feedback(file_paths)
    
    # Combine into one master file
    if output_files:
        current_date = datetime.now().strftime('%Y%m%d')
        master_output_path = os.path.join(
            output_directory,
            f"MASTER_NSE_CM_STOCKS_EOD_{current_date}.csv"
        )
        master_created = combine_all_outputs_into_master(output_files, master_output_path)
        
        # Delete individual outputs if master is created
        if master_created:
            delete_individual_outputs(output_files)
    else:
        master_output_path = None
    
    # Show summary
    show_results(output_files, master_output_path)

if __name__ == "__main__":
    main()

