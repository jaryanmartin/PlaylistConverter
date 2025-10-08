import argparse
import csv
import sys
import time
from pathlib import Path
from typing import List, Tuple, Optional

from dotenv import load_dotenv
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth

def read_apple_playlist_txt(path: Path) -> List[Tuple[str, str, Optional[str]]]:
    """
    Reads a Music.app exported 'Text' playlist (tab-separated).
    Returns list of (track, artist, album).
    Accepts files where headers may vary; tries common names.
    """
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Dialect
        try:
            dialect = sniffer.sniff(sample, delimiters="\t,;")
        except csv.Error:
            class Tsv(csv.Dialect):
                delimiter = "\t"; quotechar = '"'; escapechar = None
                doublequote = True; skipinitialspace = False; lineterminator = "\n"; quoting = csv.QUOTE_MINIMAL
            dialect = Tsv
        reader = csv.DictReader(f, dialect=dialect)

        def pick(row, keys):
            for k in keys:
                if k in row and row[k].strip():
                    return row[k].strip()
            return ""

        for row in reader:
            title = pick(row, ["Name", "Title", "Track Name"])
            artist = pick(row, ["Artist", "Artist Name"])
            album  = pick(row, ["Album", "Album Title", "Album Name"])
            if title and artist:
                rows.append((title, artist, album or None))
    return rows

def best_spotify_match(sp: spotipy.Spotify, title: str, artist: str, album: Optional[str]) -> Optional[str]:
    """
    Try a few search queries from strict → relaxed. Returns a Spotify track ID or None.
    """
    queries = []
    if album:
        queries.append(f'track:"{title}" artist:"{artist}" album:"{album}"')
    queries.append(f'track:"{title}" artist:"{artist}"')
    queries.append(f'{title} {artist}')
    queries.append(title)

    for q in queries:
        results = sp.search(q=q, type="track", limit=5)
        items = results.get("tracks", {}).get("items", [])
        if items:
            return items[0]["id"]
        time.sleep(0.05)  
    return None

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

# -------- Main --------
def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Convert Apple Music (exported text) playlist to Spotify playlist.")
    parser.add_argument("--in", dest="infile", required=True, help="Path to exported Apple Music playlist (.txt/.csv)")
    parser.add_argument("--playlist", dest="playlist_name", required=True, help="Name of Spotify playlist to create/use")
    parser.add_argument("--public", action="store_true", help="Create playlist as public (default private)")
    args = parser.parse_args()

    infile = Path(args.infile).expanduser().resolve()
    if not infile.exists():
        print(f"Input file not found: {infile}", file=sys.stderr)
        sys.exit(1)

    username = os.getenv("SPOTIFY_USERNAME")
    if not username:
        print("Missing SPOTIFY_USERNAME in .env", file=sys.stderr)
        sys.exit(1)

    scope = "playlist-modify-public playlist-modify-private"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope, open_browser=True))

    rows = read_apple_playlist_txt(infile)
    if not rows:
        print("No tracks found in the exported file. Make sure you chose File → Library → Export Playlist… (Text).")
        sys.exit(1)
    print(f"Loaded {len(rows)} tracks from Apple export.")

    current_user = sp.current_user()["id"]
    target_playlist_id = None

    limit = 50
    offset = 0
    while True:
        pls = sp.current_user_playlists(limit=limit, offset=offset)
        for p in pls["items"]:
            if p["name"] == args.playlist_name and p["owner"]["id"] == current_user:
                target_playlist_id = p["id"]
                break
        if target_playlist_id or not pls["next"]:
            break
        offset += limit

    if not target_playlist_id:
        created = sp.user_playlist_create(
            user=current_user,
            name=args.playlist_name,
            public=args.public,
            description="Imported from Apple Music export"
        )
        target_playlist_id = created["id"]
        print(f'Created playlist: {args.playlist_name}')
    else:
        print(f'Adding to existing playlist: {args.playlist_name}')

    track_ids = []
    misses = []
    for i, (title, artist, album) in enumerate(rows, 1):
        tid = best_spotify_match(sp, title, artist, album)
        if tid:
            track_ids.append(tid)
        else:
            misses.append((title, artist, album))
        if i % 25 == 0:
            print(f"Matched {i}/{len(rows)}…")

    for chunk in chunked(track_ids, 100):
        sp.playlist_add_items(target_playlist_id, chunk)

    print(f"Added {len(track_ids)} tracks to '{args.playlist_name}'.")
    if misses:
        print(f"\nCould not match {len(misses)} tracks. See 'misses.csv'.")
        with open("misses.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Title", "Artist", "Album"])
            w.writerows(misses)

if __name__ == "__main__":
    main()
