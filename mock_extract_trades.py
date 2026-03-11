import sys
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="20240101")
    parser.add_argument("--image", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("Running Mock Extract Trades...")
    user_input = input("Enter 'q' to quit (exit 2) or 'y' to succeed (exit 0): ").strip()
    
    if user_input.lower() == 'q':
        print("Cancelled by mock user.")
        sys.exit(2)
    else:
        print("Mock success.")
        sys.exit(0)

if __name__ == "__main__":
    main()
