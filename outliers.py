import sys

def main():
    path = "output_diag/train/token_frequencies.tsv"
    
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip('\n')
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 6:
                    surf = parts[0]
                    norm = parts[1]
                    try:
                        c_c = int(parts[4])
                        # Filter to tokens that have at least some basic presence to prevent rare spelling noise
                        if parts[5] == "x" or c_c < 100:
                            continue
                        
                        # Only compare identical surface/norm forms to avoid contrasting
                        # a rare misspelling's observed rank against the root word's dictionary rank!
                        if surf != norm:
                            continue
                            
                        chive_p = float(parts[5])
                        rows.append({
                            "surf": surf,
                            "c_c": c_c,
                            "chive_p": chive_p
                        })
                    except ValueError:
                        continue
    except FileNotFoundError:
        print(f"File not found: {path}")
        print("Please run ./sync_diagnostics.sh first!")
        return

    # Sort descending by corpus count to accurately establish corpus percentile
    rows.sort(key=lambda x: x["c_c"], reverse=True)
    total = len(rows)
    
    outliers = []
    for r, row in enumerate(rows):
        # r=0 is highest count -> 100.0%. r=total is lowest count -> 0.0%
        observed_p = 100.0 * (1.0 - (r / total))
        diff = observed_p - row["chive_p"]
        row["observed_p"] = observed_p
        row["diff"] = diff
        row["abs_diff"] = abs(diff)
        outliers.append(row)

    # Sort strictly by the widest gap in percentiles
    outliers.sort(key=lambda x: x["abs_diff"], reverse=True)

    print(f"Total statistically relevant tokens evaluated (>100 freq): {total:,}")
    print("=" * 100)
    print(f"{'Rank':<5} {'Token':<15} {'Observed %':<12} {'chiVe %':<12} {'Gap %':<12} {'Corpus Count'}")
    print("-" * 100)
    for i, row in enumerate(outliers[:20]):
        print(f"{i+1:<5} {row['surf']:<15} {row['observed_p']:<12.2f} {row['chive_p']:<12.2f} {row['diff']:<+12.2f} {row['c_c']:,}")

if __name__ == "__main__":
    main()
