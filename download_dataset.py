import os
# pyrefly: ignore [missing-import]
from datasets import load_dataset

def main():
    print("Loading dataset...")
    # Usar un directorio temporal muy corto para evitar errores de ruta larga en Windows (MAX_PATH = 260)
    cache_dir = "C:/Users/celia/c"
    os.makedirs(cache_dir, exist_ok=True)
    
    ds = load_dataset(
        "electricsheepafrica/africa-synth-cancer-breast-cancer-genomics-ssa-all",
        cache_dir=cache_dir
    )
    
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files", "dataset")
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Saving dataset to {output_dir}...")
    ds.save_to_disk(output_dir)
    print("Done!")

if __name__ == "__main__":
    main()
