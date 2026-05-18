#!/usr/bin/env bash
# Compute BLAKE3 of every media file in the corpus subset, output to
# corpus-source.blake3 (one line per file: hash\trelative-path).
set -euo pipefail
ROOT="${1:?root dir}"
SUBSET_FILE="${2:?subset file list}"
OUTPUT="${3:?output blake3 file}"

cd "$ROOT"
> "$OUTPUT"
while IFS= read -r folder; do
    [ -z "$folder" ] && continue
    # Strip trailing slash
    folder="${folder%/}"
    if [ ! -d "$folder" ]; then
        echo "WARN: missing folder $folder" >&2
        continue
    fi
    find "$folder" -type f \( -name "*.mkv" -o -name "*.mp4" -o -name "*.plexmatch" \) -print0 \
        | xargs -0 -P 4 -I {} bash -c 'h=$(b3sum --no-names "{}" 2>/dev/null) && printf "%s\t%s\n" "$h" "{}"' >> "$OUTPUT"
done < "$SUBSET_FILE"
echo "DONE: $(wc -l < "$OUTPUT") files hashed"
