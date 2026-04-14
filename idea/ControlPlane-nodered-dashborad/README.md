# NodeFlow — React Auth Dashboard for Node-RED

A production-ready React app connecting Node-RED with Google OAuth, Facebook OAuth, and local authentication.

## Project Structure

```
nodered-auth-app/
├── public/index.html
├── src/
│   ├── context/AuthContext.jsx   ← All auth logic (Google, Facebook, local)
│   ├── hooks/useNodeRed.js       ← Node-RED WebSocket + flow data
│   ├── pages/
│   │   ├── LoginPage.jsx         ← Auth UI (all 3 providers)
│   │   └── DashboardPage.jsx     ← Flow monitor + debug logs
│   ├── App.jsx                   ← Router + protected routes
│   └── index.js
├── node-red-auth-flows.json      ← Import this into Node-RED
└── package.json
```

## Quick Start

### 1. Install & run the React app

```bash
cd nodered-auth-app
npm install
npm start
# Opens at http://localhost:3000
```

### 2. Import flows into Node-RED

1. Open Node-RED → Menu → Import
2. Paste contents of `node-red-auth-flows.json`
3. Click Import → Deploy

### 3. Local auth works immediately

- Go to http://localhost:3000
- Enter any email + password (min 6 chars)
- Dashboard opens in demo mode if Node-RED isn't running

---

## Google OAuth Setup

1. Go to https://console.cloud.google.com
2. Create a project → APIs & Services → Credentials
3. Create OAuth 2.0 Client ID (Web application)
4. Add authorized redirect URI: `http://localhost:1880/auth/google/callback`
5. Copy Client ID and Client Secret
6. In Node-RED settings.js, add:

```js
process.env.GOOGLE_CLIENT_ID     = 'your-client-id';
process.env.GOOGLE_CLIENT_SECRET = 'your-client-secret';
```

Or set via Node-RED environment variables in the function node.

---

## Facebook OAuth Setup

1. Go to https://developers.facebook.com
2. Create App → Consumer → Add Facebook Login product
3. Settings → Valid OAuth Redirect URIs: `http://localhost:1880/auth/facebook/callback`
4. Copy App ID and App Secret
5. In Node-RED, set:

```js
process.env.FACEBOOK_APP_ID     = 'your-app-id';
process.env.FACEBOOK_APP_SECRET = 'your-app-secret';
```

---

## Node-RED Auth Endpoints

| Method | Endpoint           | Purpose                        |
|--------|--------------------|--------------------------------|
| POST   | /auth/local        | Email/password login           |
| POST   | /auth/register     | New user registration          |
| GET    | /auth/google       | Google OAuth redirect          |
| GET    | /auth/facebook     | Facebook OAuth redirect        |
| GET    | /auth/*/callback   | OAuth callback → JWT response  |

### Expected response format (POST /auth/local):

```json
{
  "token": "eyJhbGc...",
  "user": {
    "id": "user-123",
    "name": "Alice",
    "email": "alice@example.com",
    "provider": "local",
    "avatar": "https://...",
    "role": "admin"
  }
}
```

---

## Environment Variables

Create `.env` in the project root:

```
REACT_APP_NR_URL=http://localhost:1880
```

---

## Dashboard Features

**Flow Canvas**
- Live SVG flow visualization with animated connectors
- Per-node status: idle / running / done / warn / error
- Execution time overlays and sparkline history
- Per-node timing bars

**Debug Logs**
- Filterable by level (DEBUG / INFO / WARN / ERROR)
- Full-text search
- Payload inspector on click
- WebSocket live feed from Node-RED

**Config Tab**
- Node-RED URL configuration
- Auth provider status overview
- Setup guide for Google & Facebook
