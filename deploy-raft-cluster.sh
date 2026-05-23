#!/bin/bash

# ============================================================================
# Raft Cluster Auto-Deploy Script
# Deploys and starts 3-node Raft cluster on: gengar-1, oddish-1, charmander-5
# ============================================================================

# Default values (can be overridden)
PROJECT_PATH="${1:-.}"
NODE1="${2:-bulbassaur-3}"
NODE2="${3:-oddish-1}"
NODE3="${4:-charmander-5}"

PROJECT_BASE="$PROJECT_PATH/distalg-project2-base"
CONFIG_FILE="$PROJECT_BASE/src/main/resources/babel_config.properties"
NODES=("$NODE1" "$NODE2" "$NODE3")
BABEL_PORT=34000
SERVER_PORT=35000

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}========================================"
echo -e "Raft Cluster Deployment Script"
echo -e "========================================${NC}"
echo ""

# Step 1: Update babel_config.properties
echo -e "${YELLOW}[1/4] Updating babel_config.properties...${NC}"
INITIAL_MEMBERSHIP=""
for node in "${NODES[@]}"; do
    if [ -z "$INITIAL_MEMBERSHIP" ]; then
        INITIAL_MEMBERSHIP="${node}:${BABEL_PORT}"
    else
        INITIAL_MEMBERSHIP="${INITIAL_MEMBERSHIP},${node}:${BABEL_PORT}"
    fi
done

cat > "$CONFIG_FILE" << EOF
#### Agreement
agreement_proto_id=100

#### StateMachine
babel.port=$BABEL_PORT
initial_membership=$INITIAL_MEMBERSHIP

#### App
server_port=$SERVER_PORT

##### General
babel.interface=eth0
EOF

echo -e "${GREEN}✓ Config updated: initial_membership=$INITIAL_MEMBERSHIP${NC}"
echo ""

# Step 2: Build the JAR
echo -e "${YELLOW}[2/4] Building project (mvn clean package)...${NC}"
cd "$PROJECT_BASE"
mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Build failed!${NC}"
    exit 1
fi
cd - > /dev/null
echo -e "${GREEN}✓ JAR built: $PROJECT_BASE/target/DistAlg.jar${NC}"
echo ""

# Step 3: Deploy to each node
echo -e "${YELLOW}[3/4] Deploying to cluster nodes...${NC}"
for node in "${NODES[@]}"; do
    echo -e "  ${CYAN}Copying to $node...${NC}"
    scp -r "$PROJECT_BASE" "${node}:~/DistAlg" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "  ${GREEN}✓ $node deployment complete${NC}"
    else
        echo -e "  ${RED}✗ Failed to deploy to $node${NC}"
    fi
done
echo ""

# Step 4: Start Raft instances on each node
echo -e "${YELLOW}[4/4] Starting Raft instances on each node...${NC}"
echo ""

for node in "${NODES[@]}"; do
    echo -e "  ${CYAN}Starting on $node (logging to /tmp/raft_${node}.log)...${NC}"

    REMOTE_CMD="cd ~/DistAlg/distalg-project2-base && java -jar target/DistAlg.jar babel.address=$node babel.port=$BABEL_PORT initial_membership=$INITIAL_MEMBERSHIP server_port=$SERVER_PORT"

    ssh -n "$node" "nohup bash -c '$REMOTE_CMD' > /tmp/raft_${node}.log 2>&1 &" 2>/dev/null

    if [ $? -eq 0 ]; then
        echo -e "  ${GREEN}✓ Started on $node${NC}"
    else
        echo -e "  ${RED}✗ Failed to start on $node${NC}"
    fi
done

echo ""
echo -e "${CYAN}========================================"
echo -e "Deployment Complete!"
echo -e "========================================${NC}"
echo ""
echo -e "${YELLOW}View logs on each node:${NC}"
for node in "${NODES[@]}"; do
    echo -e "  ${CYAN}ssh $node 'tail -f /tmp/raft_${node}.log'${NC}"
done
echo ""
echo -e "${YELLOW}To stop all instances:${NC}"
echo -e "  ${CYAN}ssh <node> 'pkill -f DistAlg.jar'${NC}"
echo ""
echo -e "${YELLOW}Waiting 10 seconds, then showing first logs from each node...${NC}"
sleep 10

for node in "${NODES[@]}"; do
    echo ""
    echo -e "${CYAN}--- $node logs (last 20 lines) ---${NC}"
    ssh "$node" "tail -20 /tmp/raft_${node}.log" 2>/dev/null
done

echo ""
echo -e "${CYAN}========================================"
echo -e "${GREEN}✓ Check logs above for 'Leader changed: null -> <node>' to confirm consensus!${NC}"
echo -e "========================================${NC}"

