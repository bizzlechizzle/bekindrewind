cleanup.py

Rules

1. We are working with Python Scripts
2. Scripts are universal and run on any OS.
3. We always value code that is KISS, yet bulletproof, and verified with ULTRATHINK.
4. We respect the text case of the original folders, online lookups, and API lookups.
5. We follow common normalization for TORRENTING.
6. We dont use emojis or leave un-needed comments.
7. The terminal interface is KISS, nothing extra needed.
8. the database is based on either movies or tv shows, check database.py for more information
9. when troubleshootig check other scripts or .md to understand how they work

Overview

After successful torrent uploads, cleanup the original source files to save disk space and avoid duplicates.

This script:
- Finds all successfully uploaded files (uploaded = 1 in database)
- Deletes original source files from streaming platforms (Amazon, YouTube, HBO, Max, Netflix, Hulu)
- Removes empty directories
- Only touches files from the configured source location
- Preserves hardlinked files in upload folders

Safety:
- Only deletes files that have been successfully uploaded as torrents
- Only deletes from configured source directories
- Verifies hardlinks exist before deletion
- Reports all deletions in verbose mode

Arguments
-v verbose mode