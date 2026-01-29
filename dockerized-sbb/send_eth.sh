#!/bin/bash

# 1. 환경 설정
CAST="$HOME/.foundry/bin/cast"
RPC="http://127.0.0.1:8545"
PK="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# 2. 보낼 주소들 (여기에 4개의 주소를 입력하세요)
ADDRESSES=(
    "0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f"
    "0xa0Ee7A142d267C1f36714E4a8F75612F20a79720"
    "0x71bE63f3384f5fb98995898A86B02Fb2426c5788"
    "0xcd3B766CCDd6AE721141F452C550Ca635964ce71"
    "0xFABB0ac9d68B0B445fB7357272Ff202C5651694a"
    "0x2546BcD3c84621e976D8185a91A922aE77ECEc30"
    "0x1CBd3b2770909D4e10f157cABC84C7264073C9Ec"
    "0xbDA5747bFD65F08deb54cb465eB87D40e51B197E"
    "0xdF3e18d64BC6A983f673Ab319CCaE4f1a57C7097"
    "0xdD2FD4581271e230360230F9337D5c0430Bf44C0"
)

echo "🚀 전송 및 잔액 확인 작업을 시작합니다..."

for ADDR in "${ADDRESSES[@]}"
do
    echo "===================================================="
    echo "📍 대상 주소: $ADDR"
    
    # [Step 1] 전송 전 잔액 확인
    PRE_BALANCE=$($CAST balance "$ADDR" --ether --rpc-url "$RPC")
    echo "💰 전송 전 잔액: $PRE_BALANCE ETH"
    
    echo "📦 10 ETH 전송 중..."
    
    # [Step 2] 전송 실행 (성공 시에만 다음 단계 진행)
    if $CAST send "$ADDR" --value 10ether --private-key "$PK" --rpc-url "$RPC" > /dev/null; then
        echo "✅ 전송 성공!"
        
        # [Step 3] 전송 후 잔액 확인
        POST_BALANCE=$($CAST balance "$ADDR" --ether --rpc-url "$RPC")
        echo "💵 전송 후 잔액: $POST_BALANCE ETH"
    else
        echo "❌ 전송 실패! 이 주소에서 문제가 발생하여 스크립트를 중단합니다."
        exit 1
    fi
done

echo "===================================================="
echo "🎉 모든 주소에 대해 전송 및 확인이 완료되었습니다!"
