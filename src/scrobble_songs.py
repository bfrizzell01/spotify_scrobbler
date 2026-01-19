import pylast
from datetime import datetime
import click
import time
import json
import os
from pathlib import Path
import random
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

username = os.getenv('LASTFM_USERNAME')
password = os.getenv('LASTFM_PASSWORD')

if not username or not password or not API_KEY or not API_SECRET:
    raise ValueError("LASTFM_USERNAME, LASTFM_PASSWORD, API_KEY, API_SECRET must be set in .env file")

password_hash = pylast.md5(password)

def convert_to_unix_timestamp(time_str):
    """Convert ISO 8601 timestamp string to UNIX timestamp"""
    dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt.timestamp())

def append_json(file_path, new_data):
    path = Path(file_path)

    # Load existing data
    if path.exists():
        with path.open("r") as f:
            data = json.load(f)
    else:
        data = []

    # Append new failures
    data.extend(new_data)

    # Write back atomically
    with path.open("w") as f:
        json.dump(data, f, indent=2)

    return

def scrobble_with_retry(network, song, max_retries=5):
    """Attempt to scrobble with exponential backoff on failure"""
    
    for attempt in range(max_retries):
        try:
            network.scrobble(
                artist=song['artistName'],
                title=song['trackName'],
                timestamp=song.get('timestamp', 1767253534),  # Backdated timestamp
                album=song.get('albumName', '')
            )
            return True, None
            
        except (pylast.NetworkError, pylast.MalformedResponseError, pylast.WSError) as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit or server error
            if any(code in error_str for code in ['502', '500', '503', 'timeout', 'rate limit']):
                if attempt < max_retries - 1:
                    # Exponential backoff: 2s, 4s, 8s, 16s, 32s (capped at 60s)
                    wait_time = min(60, (2 ** (attempt + 1)))
                    click.echo(f"  Server error (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s: {e}", err=True)
                    time.sleep(wait_time)
                else:
                    return False, f"Server error after {max_retries} attempts: {e}"
            else:
                # Other errors (bad song data, etc.) - don't retry
                return False, f"API error: {e}"
                
        except Exception as e:
            return False, f"Unexpected error: {e}"
    
    return False, "Max retries exceeded"

@click.command()
@click.option('--file', type=click.File('r'), help='Filename of song batch', required=True)
@click.option('--start-index', type=int, help='Index of song to start at', required=False, default=0)
@click.option('--delay', type=float, help='Delay between requests in seconds', default=0.4)
def main(file, start_index, delay):

    # load file
    songs = json.load(file)[start_index:]

    # establish connection
    network = pylast.LastFMNetwork(
        api_key=API_KEY,
        api_secret=API_SECRET,
        username=username,
        password_hash=password_hash,
    )

    # scrobble songs
    success_count = 0
    error_count = 0
    failed_songs = []
    consecutive_errors = 0
    
    for i, song in enumerate(songs):
        success, error_msg = scrobble_with_retry(network, song)
        
        if success:
            success_count += 1
            consecutive_errors = 0
            
            # Report progress every 100 songs
            if (start_index + i + 1) % 100 == 0:
                click.echo(f"[{start_index + i + 1}/{start_index + len(songs)}] Progress: {success_count} scrobbled, {error_count} errors")
        else:
            error_count += 1
            consecutive_errors += 1
            failed_songs.append({**song, 'error': error_msg})
            click.echo(f"[{i+1}/{len(songs)}] ERROR: {song['artistName']} - {song['trackName']}: {error_msg}", err=True)
            
            # Back off more aggressively after errors
            if consecutive_errors > 3:
                wait_time = min(60, consecutive_errors * 10)  # Cap at 60s
                click.echo(f"  Multiple consecutive errors, waiting {wait_time}s...", err=True)
                time.sleep(wait_time)
        
        if consecutive_errors > 10:
            click.echo('Too many consecutive errors: Stopping.', err=True)
            break
        
        # Base delay with jitter
        time.sleep(delay + random.uniform(0, 0.2))
        
        # Longer break every 500 songs
        if (i + 1) % 500 == 0:
            click.echo(f"Processed {i+1} songs, taking a 2-minute cooldown break...")
            time.sleep(120)

    # Write results to files
    file_basename = os.path.basename(file.name)
    with open('../logs/scrobbled_files.txt', 'a') as f:
        f.write(f"{file_basename}\n")
        click.echo(f"Added {file_basename} to scrobbled_files.txt")
    
    # Write failed songs to JSON if any errors occurred
    if failed_songs:
        append_json('../logs/failed_songs.json', failed_songs)
        click.echo(f"Wrote {len(failed_songs)} failed songs to failed_songs.json", err=True)

    click.echo(
        f"Finished scrobbling {file_basename}: {success_count} successful, {error_count} failed out of {len(songs)} songs."
    )
    return

if __name__ == '__main__':
    main()