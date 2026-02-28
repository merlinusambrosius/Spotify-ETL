import json
import boto3

# ================== CONFIG ==================
BUCKET = 'justin-music-ai-experiments-2026'
KEY = 'spotify_cleaned.json'          # ← change only if you made it timestamped
# ===========================================

print("🎵 Spotify ETL Data Viewer\n" + "="*70)

try:
    # Try loading directly from S3 (easiest)
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=BUCKET, Key=KEY)
    tracks = json.loads(response['Body'].read().decode('utf-8'))
    print(f"✅ Loaded directly from S3 → {KEY} ({len(tracks)} tracks)\n")
except Exception:
    # Fallback to local file (if you downloaded it)
    with open('spotify_cleaned.json', 'r', encoding='utf-8') as f:
        tracks = json.load(f)
    print(f"✅ Loaded from local file ({len(tracks)} tracks)\n")

# Pretty print
print("="*70)
for i, track in enumerate(tracks, 1):
    name   = track.get('name',   'N/A')
    artist = track.get('artist', 'N/A')
    album  = track.get('album',  'N/A')
    
    print(f"{i:2d}. {name}")
    print(f"    🎤 Artist : {artist}")
    print(f"    💿 Album  : {album}")
    print("─" * 60)

print("="*70)
print(f"✅ Total tracks displayed: {len(tracks)}")