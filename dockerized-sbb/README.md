## General Overview

**dockerized-sbb** repository consists of the following components each of which run in a separate docker container:

- **Seeder**
- **Key Generator**
- **TX Orderer**
- **Secure RPC Provider**
- **Anvil _(optionally cloned if ./network_tasks.sh is executed)_**

Anvil is where the **LivenessServiceManager** and **ValidationServiceManager** contracts will be deployed.

- **Network Owners** deploy the contracts and whitelist operators.
- **Operators** register and opt into services.

---

## 👽 **Operator Setup**

Operators are only responsible for running the **TX Orderer** service. The process is simplified and doesn't require deploying contracts or starting the entire network.

---

### **Step 0: Copy & Configure Environment Variables**

Copy the required environment files from the `./env_templates` directory into the project root:

```bash
cp env_templates/.env_template .env
cp env_templates/.operator_env_template .operator_env

```

Then, **edit both `.env` and `.operator_env` manually**, filling in the necessary values (e.g., RPC URLs, contract addresses, private keys, etc.).

---

_Note:_ Each of the four main components can run in two modes:

- **`init`** – Sets up configuration files in individual containers (Run this first).
- **`run`** – Used for subsequent executions unless `.env` settings are changed.

---

### **Step 1: Run Operator Setup Script**

Execute the operator setup script:

```bash
./05_operator_tasks.sh
```

This script will:

✅ Check the Foundry `cast` installation and install the correct version if needed.

✅ Register the operator.

✅ Opt into the **vault** and **network**.

✅ Register **TX Orderer** with the **LivenessServiceManager**.

> ⚠️ Make sure you have received the .env file and contract details from the network owner before proceeding.

---

### **Step 2: Start Docker Interactive Menu**

Start the service selector:

```bash
./06_run_docker.sh
```

You will be prompted with:

```
SELECT YOUR OPERATION MODE
==============================
0) Exit
1) Run Tx Orderer as Operator
2) Run All Services as Developer
```

Select option `1`.

---

### **Step 3: TX Orderer Menu for Operator**

After selecting Operator mode, you’ll see:

```
========================
 TX_ORDERER - OPERATOR
========================
0) Exit
1) Build Tx Orderer
2) Run Tx Orderer
------------------------
```

- Choose `1` to **Build** the TX Orderer container (only needed once or when source changes).
- Then choose `2` to **Run** the TX Orderer.

You can skip building if you have already done it previously.

---

## 👩‍💻 **Developer (Network Owner) Setup**

Developers are responsible for deploying smart contracts, configuring the environment, staking collateral, and running all services necessary for the network to function.

---

### **Step 0: Copy & Configure Environment Variables**

Copy the `.env` file from the `./env_templates` directory into the project root:

```bash
cp env_templates/.env_template .env
```

Then, **edit `.env` manually**, filling in the necessary values such as RPC URLs, Anvil settings, contract repo config, etc.

---

> Note: Each of the four main components can run in two modes:
>
> - **`init`** – Sets up configuration files in individual containers (Run this first).
> - **`run`** – Used for subsequent executions unless `.env` settings are changed.

---

### **Step 1: Deploy Contracts & Start Blockchain**

Run the blockchain tasks script:

```bash
./01_run_blockchain_tasks.sh
```

This script provides an interactive menu with the following options:

```
========================
 MENU
========================
0) Exit
1) Deploy Contracts
2) Start blockchain
------------------------

```

- **Option 1: Deploy Contracts**
  - Clones the `symbiotic-middleware-contract` repository if not already cloned.
  - Replaces environment variables in the repository's internal `env.sh` file using your `.env`.
  - Builds and deploys all contracts.
  - Exports the contract addresses and deployment data into a local file:
    **`deployed_info.sh`**
- **Option 2: Start Blockchain**
  - Starts the blockchain process via `make start`, using the values from `deployed_info.sh`.

---

### **Step 2: Populate Project Environment Files**

Once contracts are deployed, copy the example env files to the root of your project:

```bash
cp env_template/.vault_env_template .vault_env
cp env_template/.staker_env_template .staker_env
cp env_template/.network_env_template .network_env
cp env_template/.operator_env_template .operator_env

```

Manually edit each of these newly created files by copying the appropriate values from `deployed_info.sh`.

This ensures all subsystems (vault, staker, network, operator) are properly configured to interact with the deployed contracts.

---

### **Step 3: Stake Using Staker Script**

With `.staker_env` configured, run:

```bash
./02_run_staker_tasks.sh

```

You will be prompted with:

```
========================
 MENU
========================
0) Exit
1) Get token (for testing)
2) Stake
------------------------

```

- **Option 1: Get Token**
  - Transfers mock tokens to the staker address for testing purposes.
  - Verifies the staker’s balance.
- **Option 2: Stake**
  - Approves the collateral contract to spend tokens on behalf of the staker.
  - Deposits tokens into the collateral contract.
  - Approves the vault contract to spend the collateral.
  - Deposits the collateral into the vault contract.
  - Confirms the active shares for the staker in the vault.

---

### **Step 4: Initialize the Network**

Run the network initialization script:

```bash
./03_run_network_tasks.sh
```

This script manages cluster creation, rollup registration, and sets up key network components. You'll see:

```
========================
 MENU
========================
0) Exit
1) Initialize Cluster & Add rollup
2) Register network and set middleware
3) Register operator
4) Register token
5) Register vault
6) Set max network limit
7) Execute all process (1 ~ 6)
------------------------

```

- **Option 1**: Creates the **Cluster** and **Rollup**, if not already initialized.
- **Option 2**: Registers the **Network** and links it to the **Middleware**.
- **Option 3**: Registers the **Operator** to the **Validation Service Manager**.
  > ⚠️ This step may fail if you haven’t run 05_operator_tasks.sh. That’s okay — once you run 05_operator_tasks.sh afterward it should be resolved.
- **Option 4**: Registers the **Token** used in validation.
- **Option 5**: Registers the **Vault**, including staker/operator reward addresses.
- **Option 6**: Sets the maximum network limit via the Delegator contract.
- **Option 7**: Runs all of the above steps sequentially.

---

### **Step 5: Set Vault Delegation Parameters**

Once your `.vault_env` is properly set up, run:

```bash
./04_run_vault_tasks.sh
```

This script allows the vault owner to manage the network limit and operator shares within the Delegator contract.

You will see the following menu:

```
========================
 MENU
========================
0) Exit
1) Set network limit (vault)
2) Set network share (vault)
------------------------
```

- **Option 1: Set Network Limit (Vault)**
  - Calls `setNetworkLimit` on the Delegator contract.
  - This defines the maximum amount of delegation allowed for the specified subnetwork.
  - If the limit is already set, it prints the current values for reference.
- **Option 2: Set Network Share (Vault)**
  - Sets the operator’s share in the given subnetwork using `setOperatorNetworkShares`.
  - Also calls `stake(...)` to retrieve the current delegated amount.
  - If the share is already set, it will notify without error.

---

### **Step 6: Register the Operator and TX Orderer**

Even though this step is run by the **operator**, developers often execute this during local or devnet setups to simulate a complete environment.

Run:

```bash
./05_operator_tasks.sh

```

This script automates several critical steps to register the operator in all the necessary systems.

---

### Here's what it does:

1. **Check and Install Foundry**
   - If `cast` is missing, it installs Foundry and updates it to the required **nightly version**:
     ```
     cast 0.2.0 (5b7e4cb 2023-12-02T00:23:06.394266000Z)

     ```
2. **Register the Operator**
   - Calls the `registerOperator()` function if the operator isn’t already registered.
3. **Opt-in to the Vault**
   - Uses the `OperatorVaultOptInService` to opt the operator into the vault contract.
4. **Opt-in to the Network**
   - Uses the `OperatorNetworkOptInService` to opt the operator into the network.
5. **Register TX Orderer**
   - Registers the `TX_ORDERER_ADDRESS` under the given `CLUSTER_ID` with the `LivenessServiceManager`.

Each step checks if the operation has already been completed and only executes if necessary.

> ✅ When this script is run after 03_run_network_tasks.sh, any errors during the earlier register_operator step will now be resolved, and the operator will be correctly onboarded into the system.

---

### **Step 7: Build & Run Services via Docker Orchestrator**

Once all components are configured and registered, launch the orchestrator script:

```bash
./06_run_docker.sh

```

This script provides an interactive interface to build and run services based on your role.

---

### 👽 Operator Mode

If you're running only the **TX Orderer**, select:

```
1) Run Tx Orderer as Operator

```

You will see:

```
========================
 TX_ORDERER - OPERATOR
========================
0) Exit
1) Build Tx Orderer
2) Run Tx Orderer
------------------------

```

- `1`: Build the `tx_orderer` Docker container.
- `2`: Run the `tx_orderer` service.
- Use `2` directly if it's already built.

---

### 👩‍💻 Developer Mode

If you're the **network owner** or need to run all services, select:

```
2) Run All Services as Developer

```

You’ll see a comprehensive menu:

```
=============================
 DOCKER SERVICE ORCHESTRATOR
=============================
0) Exit
1) Seeder
2) Distributed Key Generator
3) Tx Orderer
4) Secure RPC Provider
5) Run All Services (no build)
6) Build & Run All Services
-----------------------------

```

- **1–4**: Select and manage each service individually.
  - Choose to **build** or **run** each component as needed.
- **5**: Run all services (only if already built).
- **6**: Build and run all services together.

This script uses `docker-compose` to manage each container individually, allowing modular workflows and easy debugging.

### 🗺️ **Accurate Developer and Operator Setup Diagram**

```
                    +-----------------------------------------+
                    | 1. Copy & Configure .env                |
                    |    from /env_template/.env_template     |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 2. Run 01_run_blockchain_tasks.sh       |
                    |  - Deploy Contracts                     |
                    |  - Start Anvil                          |
                    |  - Output: deployed_info.sh             |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 3. Copy & Configure the following files |
                    |    from /env_template/ into root:       |
                    |    - .vault_env                         |
                    |    - .staker_env                        |
                    |    - .network_env                       |
                    |    - .operator_env                      |
                    |  ↪ Fill them using deployed_info.sh     |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 4. Run 02_run_staker_tasks.sh           |
                    |  - Get token (test)                     |
                    |  - Stake into Collateral & Vault        |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 5. Run 03_run_network_tasks.sh          |
                    |  - Init Cluster & Rollup                |
                    |  - Register Network, Vault, Token       |
                    |  - Set max network limit                |
                    |  - (Operator registration may fail)     |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 6. Run 04_run_vault_tasks.sh            |
                    |  - Set Vault's network limit            |
                    |  - Set operator network share           |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 7. Run 05_operator_tasks.sh             |
                    |  - Register Operator                    |
                    |  - Opt-in to Vault & Network            |
                    |  - Register Tx Orderer                  |
                    +-----------------------------------------+
                                       |
                                       v
                    +-----------------------------------------+
                    | 8. Run 06_run_docker.sh                 |
                    |                                         |
                    |  ┌──────────────────────────────────┐  |
                    |  │ Operator Mode                    │  |
                    |  │ - Build Tx Orderer              │  |
                    |  │ - Run Tx Orderer                │  |
                    |  └──────────────────────────────────┘  |
                    |                                         |
                    |  ┌──────────────────────────────────┐  |
                    |  │ Developer Mode                   │  |
                    |  │ - Build/Run Seeder, DKG, etc.    │  |
                    |  │ - Build & Run All or Individually│  |
                    |  └──────────────────────────────────┘  |
                    +-----------------------------------------+

```

![image](https://github.com/user-attachments/assets/f9c8e968-dc5c-476b-8aa3-bf789140dd6b)

# More details

### **Port Assignments**

Each component follows a structured port allocation:

| Component               | Ports               |
| ----------------------- | ------------------- |
| **Seeder**              | 10001, 10002        |
| **Key Generator**       | 11001, 11002, 11003 |
| **TX Orderer**          | 11101, 11102, 11103 |
| **Secure RPC Provider** | 11111               |

### Dockerfile Overview

Each of the four components (**Seeder, Key Generator, TX Orderer, Secure RPC Provider**) follows a similar **Dockerfile** structure.

### **1. Build Stage**

- Uses **Rust 1.75** as the base image.
- Clones the repository.
- Installs dependencies needed for building the binary.
- Compiles the project in **release mode**.

```docker
FROM rust:1.75 as builder

WORKDIR /app

# Clone the repository
RUN git clone https://github.com/gylman/<REPO-NAME>

WORKDIR /app/seeder

# Install required dependencies
RUN apt-get update && apt-get install -y \
    git clang llvm-dev libclang-dev cmake pkg-config build-essential libssl-dev

# Build the binary
RUN cargo build --release

```

---

### **2. Runtime Stage**

- Uses **Ubuntu 22.04** as the base image.
- Installs `curl` for network operations.
- Copies the necessary files from the **build stage** into the container.

```docker
FROM ubuntu:22.04

WORKDIR /app/<REPO-NAME>

# Install required dependencies
RUN apt-get update && apt-get install -y curl

# Copy scripts and built binary from the builder stage
COPY --from=builder /app/<REPO-NAME>/scripts /app/<REPO-NAME>/scripts
COPY --from=builder /app/<REPO-NAME>/target/release/<REPO-NAME> /app/<REPO-NAME>/target/release/<BINARY-NAME>

```

---

### **3. Entrypoint Handling**

Each component uses an `<REPO-NAME>-entrypoint.sh` script, defined in the **docker-compose** file and located in `scripts` directory to handle the container's startup behavior:

1. **Sets up environment variables.**
2. **Generates the `Config.toml` file** based on the `.env` file.
3. **Determines the mode (`init` or `run`).**
   - `init` → Initializes configuration files.
   - `run` → Starts the component.

---
