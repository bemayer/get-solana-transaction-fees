# Solana Fees to MongoDB

Solana Fees to MongoDB is a Rust-based tool that retrieves transaction fee data from the Solana blockchain and stores it in a local MongoDB instance. It efficiently processes transactions in batches, tracks processed blockchain slots to avoid duplicates, and ensures reliable data storage for analysis and monitoring purposes.

## Prerequisites

Before running this tool, you need to have MongoDB installed and running on your system.

### Installing MongoDB

1. For Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y mongodb
```

2. For macOS using Homebrew:
```bash
brew tap mongodb/brew
brew install mongodb-community
```

3. Start MongoDB service:
- Ubuntu/Debian: `sudo systemctl start mongodb`
- macOS: `brew services start mongodb-community`

Verify MongoDB is running:
```bash
mongosh
```

The tool expects MongoDB to be running on `localhost:27017`. If your MongoDB instance uses different connection parameters, update them in the configuration.
