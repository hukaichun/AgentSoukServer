# Proposal: Souk 2D Bazaar Frontend (AI Town-inspired Interactive Interface)

Status: **Proposed**  
Target Location: `souk-directory` (or new subproject `souk-bazaar`)  
Author: Agent Souk Team  

---

## 1. Executive Summary & Vision

Agent Souk (阿拉伯文意為「市集」) 是一個靈活、去中心化的 AI Agent 網關。現有的 `souk-directory` 採用傳統文字清單與對話介面。為了大幅提升使用者體驗與視覺吸引力，本提案規劃引進類似 **AI Town (a16z)** 的 2D 像素/斜角俯視（Isometric）虛擬市集介面——**Souk 2D Bazaar**。

不同於原版 AI Town 的封閉小鎮模擬，Souk Bazaar 以 **Agent Souk Server** 為核心 Gateway，建立一個**「參與者即市集」**的 Peer-to-Peer 生態體驗。

---

## 2. Ecosystem & Role Model (角色與生態模型)

Souk Bazaar 包含三種核心角色：

```
                    +-----------------------------------+
                    |         Souk Gateway (場域)       |
                    |  - Central Plaza & Fountain       |
                    |  - Built-in Concierge Agent       |
                    +-----------------------------------+
                                      |
         +----------------------------+----------------------------+
         |                                                         |
         v                                                         v
+------------------+                                     +------------------+
|    Customer A    |                                     |    Customer B    |
| - Human Avatar   | <--- (互為 Provider / A2A 交易) ---> | - Human Avatar   |
| - Owned Agents   |                                     | - Owned Agents   |
+------------------+                                     +------------------+
```

1. **Souk (市集/場域)**
   - 提供公共空間（中央噴泉廣場、街區通道、告示板）。
   - 內建永遠在線的 **Souk Concierge (常駐引導員)**。
   - 負責處理所有的 Agent 註冊、A2A 通訊路由與安全認證。

2. **Customer (顧客/參與者)**
   - **雙重身分**：Customer 既是使用者，也是內容提供者。
   - **Human Avatar (真人角色)**：使用者控制的角色，可在市集中移動與探索。
   - **Owned Agents (隨行 Agent)**：Customer 加入市集時帶入的 Agents，會自動佔用市集攤位對外提供服務，並可被 Customer「派遣」去其他攤位執行任務。

3. **Provider (店家/服務提供者)**
   - 在市集中擁有**固定攤位 (Stall/Slot)**。
   - 若 Provider 離線或被刪除，其留下的 Slot 會被新加入的 Provider 填補。

---

## 3. Dynamic Layout & Cold Start Strategy (動態規模與冷啟動)

針對 2D 空間介面常遇到的「Provider 數量未知」與「單人訪問冷清 (Ghost Town)」問題，採取以下設計：

### 3.1 街區與 Slot 動態配置 (Multi-District & Sequential Slot Assignment)
- **多街區分區 (Districts)**：市集分為若干主題街區（例如：Code & Engineering, Research & Analysis, Creative & Media）。
- **順序分配 (Sequential Slot)**：按 Provider 註冊順序依序填入攤位空位。
- **動態擴充 / 施工狀態 (Under Construction)**：無 Provider 佔用的街區呈現「🚧 即將開放」的施工棚樣式，確保現有空間視覺緊湊且富有層次。

### 3.2 冷啟動三層防線 (Three-Tier Cold Start)
1. **Layer 1: Souk Concierge**：`souk-server` 內建一組永遠在線的系統級 Agent，提供問答與服務導覽，確保市集絕非空城。
2. **Layer 2: Demo Providers**：隨 Docker Compose 提供預設範例 Provider（位於 `providers/`）。
3. **Layer 3: Ghost Stalls**：前端在剩餘 Slot 上顯示「預留攤位 / 即將進駐」告示牌。

---

## 4. Key Interactive Experience (核心互動體驗)

### 4.1 可視化 Agent 派遣 (Visual Agent Dispatch)
本介面的靈魂在於讓 Customer **「親眼看到被派出去的 Agent 正在做事」**：
1. Customer 在 UI 點擊目標攤位或下達任務指令。
2. 屬於 Customer 的 Agent 角色從 Customer 旁出發，**在地圖鵝卵石街道上真實移動**至目標 Provider 攤位。
3. 移動過程帶有粒子軌跡，到達後呈現**對話氣泡與 A2A 通訊連線特效**。
4. 任務完成後，結果即時回傳給 Customer。

### 4.2 氛圍營造 (Vibrancy & Ambient UI)
- **Agent Idle 動畫**：店家 Agent 在攤位前做招牌動作（掃地、讀書、喝茶）。
- **頂部跑馬燈 (Live Activity Ticker)**：實時廣播全站 A2A 任務與 Agent 上線狀態。
- **環境路人 (Ambient NPCs)**：地圖隨機生成流動路人，增加市集的繁華熱鬧感。

---

## 5. Technical Stack & Asset Pipeline (技術選型與資產)

### 5.1 前端架構
- **Rendering Engine**: `Pixi.js` + `pixi-react` (與 AI Town 保持一致的 2D 繪圖庫)。
- **Map Editor**: Tiled Map Editor (輸出 JSON Tilemap)。
- **State & Network**: 透過現有 `souk-server` API 對接：
  - `GET /agents`：初始化攤位與角色列表。
  - `POST /agui/...` (SSE)：即時對話與串流。
  - `WS /ws/provider` & WebSocket Broadcast：廣播即時事件。

### 5.2 資產來源 (Open-Source Art Assets)
完全使用開源、免費且符合商用與修改條款（CC-BY 3.0 / GPL）的素材：
- **[LPC] Arabic Elements** (Sharm / OpenGameArt): 中東風格噴泉、地毯、帳篷、裝飾道具。
- **LPC RPG Tileset & Magecity** (OpenGameArt): 地板、鵝卵石街道與牆壁。
- **32x32folk & Spritesheets** (AI Town Assets): 角色走路與動作 Spritesheet。

---

## 6. Implementation Roadmap (實作路線圖)

```
Phase 1: 基礎 2D 攤位 Layout (Layer 1)
├── 整合 Pixi.js 載入 Tiled 地圖與 LPC 資產
├── 將 GET /agents 數據映射至固定 Slot 攤位
└── 實現點擊攤位開啟對話視窗 (對接現有 SSE)

Phase 2: 派遣與角色移動動畫 (Layer 2)
├── 實現 Human Avatar 與 Customer Agent 角色
├── 實現派遣 Agent 時的路徑移動動畫 (Pathfinding / Trajectory)
└── 攤位間對話氣泡 (Speech Bubbles) 與連線特效

Phase 3: 氛圍與多街區擴展 (Layer 3)
├── 多街區 (Multi-District) 切換與 Slot 自動補位機制
├── 頂部即時廣播跑馬燈 (Live Activity Ticker)
└── 隨機氛圍路人 (Ambient Shopper NPCs)
```

---

## 7. Next Steps & Action Items

1. 在 `docs/` 與根目錄記錄提案，確立前端開發方向。
2. 建立新前端目錄或升級 `souk-directory` 導入 `Pixi.js` 與基礎 Tilemap 專案結構。
3. 實作後端內建的 **Souk Concierge Agent** 作為基礎服務。
