import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { app, BrowserWindow } = require('electron');
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function createWindow() {
    const win = new BrowserWindow({
        width: 1200,
        height: 800,
        webPreferences: {
            preload: path.join(__dirname, 'preload.js'),
            nodeIntegration: true,
            contextIsolation: true,
        },
    });
    // Use the env var if set (by other tools), otherwise fallback to default Vite port
    const devUrl = process.env.VITE_DEV_SERVER_URL || 'http://localhost:5173';
    if (process.env.NODE_ENV === 'development') {
        win.loadURL(devUrl);
        // Optional: Open DevTools automatically in dev
        // win.webContents.openDevTools();
    }
    else {
        win.loadFile(path.join(__dirname, '../dist/index.html'));
    }
}
app.whenReady().then(() => {
    createWindow();
    app.on('activate', () => {
        if (BrowserWindow.getAllWindows().length === 0) {
            createWindow();
        }
    });
});
app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
        app.quit();
    }
});
