import sys
import os

print("Current working directory:", os.getcwd())
print("Python path:")
for path in sys.path:
    print(f"  {path}")

print("\nTrying to find config...")
try:
    import config
    print("✅ config module found!")
except ImportError as e:
    print(f"❌ config import failed: {e}")

print("\nDirectory contents:")
for item in os.listdir('.'):
    print(f"  {item}")