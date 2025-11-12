import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import IsolationForest
import joblib
from pathlib import Path

print("Starting model training script...")

TRAINING_DIR = Path(__file__).resolve().parent
DATA_FILE = TRAINING_DIR.parent / 'data' / 'training_logs.jsonl'
MODEL_DIR = TRAINING_DIR.parent / 'spark' / 'models'
MODEL_DIR.mkdir(parents=True, exist_ok=True)

TFIDF_MODEL_PATH = MODEL_DIR / 'tfidf_model.joblib'
IFOREST_MODEL_PATH = MODEL_DIR / 'isolation_forest_model.joblib'

print(f"Loading training data from {DATA_FILE}...")

try:
    df = pd.read_json(DATA_FILE,lines=True)
except FileNotFoundError:
    print(f"ERROR: Training data file not found at {DATA_FILE}")
    print("Please run the log capture command to create it.")
    exit(1)
except Exception as e:
    print(f"Error loading data: {e}")
    print("Is the file a proper .jsonl file?")
    exit(1)

if len(df) < 500:
    print(f"ERROR: Loaded only {len(df)} logs. Not enough data to train.")
    print("Please capture more than 500 logs and try again.")
    exit(1)

print(f"Loaded {len(df)} logs for training.")
print("Pre-processing text data...")

df['eventMessage'] = df['eventMessage'].fillna('')
df['processImagePath'] = df['processImagePath'].fillna('')
df['combined_text'] = df['eventMessage'] + ' ' + df['processImagePath']

print("Training TF-IDF vectorizer...")

vectorizer = TfidfVectorizer(
    lowercase=True,       
    stop_words='english', 
    max_features=3000
)

vectorizer.fit(df['combined_text'])

joblib.dump(vectorizer, TFIDF_MODEL_PATH)
print(f"TF-IDF model saved to {TFIDF_MODEL_PATH}")

print("Transforming text data into vectors...")
X_train = vectorizer.transform(df['combined_text'])

print("Training Isolation Forest model...")

model = IsolationForest(
    n_estimators=100,    
    contamination=0.05,  
    random_state=42,     
    n_jobs=-1           
)

model.fit(X_train)

joblib.dump(model, IFOREST_MODEL_PATH)
print(f"Isolation Forest model saved to {IFOREST_MODEL_PATH}")

print("\n--- Training Complete! ---")
print("Models are now in the /spark/models/ directory.")