# bot.py - FINAL FIXED VERSION (Clean Header Screenshot + 429 Fix)
"""
Instagram Unban Monitor — Discord Bot
--------------------------------------
Tracks Instagram accounts you flag as "banned" and posts a Discord
notification with full stats + a real screenshot the moment they become reachable again.
"""

import os
import re
import asyncio
import base64
import io
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiohttp
from dotenv import load_dotenv
from flask import Flask
from threading import Thread

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "60"))
PORT = int(os.getenv("PORT", "8080"))

D1_WORKER_URL = os.getenv("D1_WORKER_URL", "").rstrip("/")
if D1_WORKER_URL and not D1_WORKER_URL.startswith(("http://", "https://")):
    D1_WORKER_URL = f"https://{D1_WORKER_URL}"
D1_API_KEY = os.getenv("D1_API_KEY", "")

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
STABLE_API_HOST = os.getenv("STABLE_API_HOST", "instagram-scraper-stable-api.p.rapidapi.com")
STABLE_API_URL = f"https://{STABLE_API_HOST}/ig_get_fb_profile.php"
INSTAGRAM120_HOST = os.getenv("INSTAGRAM120_HOST", "instagram120.p.rapidapi.com")
INSTAGRAM120_URL = f"https://{INSTAGRAM120_HOST}/api/instagram/profile"

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "")
APIFY_URL = f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/run-sync-get-dataset-items?token={APIFY_TOKEN}"

# Browserless Integration Variables
BROWSERLESS_TOKEN = os.getenv("BROWSERLESS_TOKEN", "")
INSTA_COOKIE = os.getenv("INSTA_COOKIE", "")

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


# ---------- owner-only check ----------

def is_owner():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message("This command only works in a server.", ephemeral=True)
            return False
        if interaction.user.id != interaction.guild.owner_id:
            await interaction.response.send_message("❌ Only the server owner can use this bot.", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


# ---------- keep-alive web server ----------

keep_alive_app = Flask(__name__)


@keep_alive_app.route("/")
def home():
    return "Instagram Unban Monitor is running."


def run_keep_alive():
    keep_alive_app.run(host="0.0.0.0", port=PORT)


def start_keep_alive():
    t = Thread(target=run_keep_alive)
    t.daemon = True
    t.start()


# ---------- D1 storage ----------

def _d1_headers():
    return {
        "Authorization": f"Bearer {D1_API_KEY}",
        "Content-Type": "application/json",
    }


async def api_get_tracked() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/tracked", headers=_d1_headers()) as resp:
            rows = await resp.json()
            return {
                row["username"]: {
                    "start_time": row["start_time"],
                    "recovered": bool(row.get("recovered", False)),
                    "recovered_at": row.get("recovered_at"),
                    "track_type": row.get("track_type") or "recovery",
                    "banned": bool(row.get("banned", False)),
                    "banned_at": row.get("banned_at"),
                    "fail_count": row.get("fail_count", 0) or 0,
                }
                for row in rows
            }


async def api_add_tracked(username: str, start_time: str, track_type: str = "recovery") -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked",
                headers=_d1_headers(),
                json={"username": username, "start_time": start_time, "track_type": track_type},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_add_tracked({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_add_tracked({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False


async def api_mark_recovered(username: str, recovered_at: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/update",
                headers=_d1_headers(),
                json={"username": username, "recovered_at": recovered_at},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_mark_recovered({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_mark_recovered({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False


async def api_mark_banned(username: str, banned_at: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/update",
                headers=_d1_headers(),
                json={"username": username, "banned_at": banned_at},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_mark_banned({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_mark_banned({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False


async def api_set_fail_count(username: str, fail_count: int) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/update",
                headers=_d1_headers(),
                json={"username": username, "fail_count": fail_count},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_set_fail_count({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_set_fail_count({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False


async def api_remove_tracked(username: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/tracked/delete",
                headers=_d1_headers(),
                json={"username": username},
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_remove_tracked({username}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_remove_tracked({username}) failed: {type(e).__name__}: {e}", flush=True)
        return False


async def api_get_config() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{D1_WORKER_URL}/config", headers=_d1_headers()) as resp:
            return await resp.json()


async def api_set_config(key: str, value) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{D1_WORKER_URL}/config", headers=_d1_headers(), json={key: value}
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"api_set_config({key}) failed: HTTP {resp.status} — {body}", flush=True)
                    return False
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"api_set_config({key}) failed: {type(e).__name__}: {e}", flush=True)
        return False


# ---------- Instagram status check ----------

async def _check_via_apify(username: str):
    """Primary provider - correctly identifies banned accounts."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                APIFY_URL,
                headers={"Content-Type": "application/json"},
                json={
                    "inputUrl": f"https://www.instagram.com/{username}",
                    "usernames": [username],
                },
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                if resp.status not in (200, 201):
                    print(f"[apify] @{username}: HTTP {resp.status}", flush=True)
                    return None
                
                data = await resp.json(content_type=None)
                
                if not data:
                    print(f"[apify] @{username}: empty response", flush=True)
                    return None
                
                if isinstance(data, list):
                    if len(data) == 0:
                        print(f"[apify] @{username}: empty dataset", flush=True)
                        return None
                    item = data[0]
                else:
                    item = data
                
                if isinstance(item, dict):
                    if "error" in item and item["error"]:
                        print(f"[apify] @{username}: error: {item['error']}", flush=True)
                        return None
                    if "errorDescription" in item and item["errorDescription"]:
                        print(f"[apify] @{username}: error: {item['errorDescription']}", flush=True)
                        return None
                    if item.get("status") == "error":
                        print(f"[apify] @{username}: error status", flush=True)
                        return None
                    if not item.get("username"):
                        print(f"[apify] @{username}: no username in response", flush=True)
                        return None
                    if item.get("followersCount") is None and item.get("postsCount") is None:
                        print(f"[apify] @{username}: null data fields", flush=True)
                        return None
                
                result_username = item.get("username") or item.get("user", {}).get("username")
                if not result_username:
                    print(f"[apify] @{username}: no username found", flush=True)
                    return None
                
                return {
                    "username": result_username,
                    "full_name": item.get("fullName") or item.get("full_name") or "",
                    "followers": item.get("followersCount") or item.get("followerCount") or 0,
                    "following": item.get("followsCount") or item.get("followingCount") or 0,
                    "posts": item.get("postsCount") or item.get("mediaCount") or 0,
                    "profile_pic_url": item.get("profilePicUrlHD") or item.get("profilePicUrl") or "",
                    "is_verified": bool(item.get("verified") or item.get("isVerified") or False),
                }
    except asyncio.TimeoutError:
        print(f"[apify] @{username}: TIMEOUT", flush=True)
        return None
    except Exception as e:
        print(f"[apify] @{username}: exception: {type(e).__name__}: {e}", flush=True)
        return None


async def _check_via_instagram120(username: str):
    """Fallback provider - only used if Apify fails."""
    headers = {
        "Content-Type": "application/json",
        "x-rapidapi-host": INSTAGRAM120_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                INSTAGRAM120_URL,
                headers=headers,
                json={"username": username},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    print(f"[instagram120] @{username}: HTTP {resp.status}", flush=True)
                    return None
                data = await resp.json(content_type=None)
                
                if not data:
                    print(f"[instagram120] @{username}: empty response", flush=True)
                    return None
                
                if data.get("error") or data.get("message"):
                    print(f"[instagram120] @{username}: error: {data.get('error') or data.get('message')}", flush=True)
                    return None
                
                result = data.get("result")
                if not result:
                    print(f"[instagram120] @{username}: no result", flush=True)
                    return None
                
                if "username" not in result or not result["username"]:
                    print(f"[instagram120] @{username}: no username", flush=True)
                    return None
                
                return {
                    "username": result.get("username", username),
                    "full_name": result.get("full_name") or "",
                    "followers": result.get("edge_followed_by", {}).get("count", 0),
                    "following": result.get("edge_follow", {}).get("count", 0),
                    "posts": result.get("edge_owner_to_timeline_media", {}).get("count", 0),
                    "profile_pic_url": result.get("profile_pic_url_hd") or result.get("profile_pic_url") or "",
                    "is_verified": bool(result.get("is_verified", False)),
                }
    except asyncio.TimeoutError:
        print(f"[instagram120] @{username}: TIMEOUT", flush=True)
        return None
    except Exception as e:
        print(f"[instagram120] @{username}: exception: {type(e).__name__}: {e}", flush=True)
        return None


async def check_instagram_status(username: str):
    """Check Instagram status using Apify and instagram120 fallback."""
    print(f"🔍 Checking @{username}...", flush=True)
    
    info = await _check_via_apify(username)
    if info is not None:
        print(f"✅ @{username} found via Apify", flush=True)
        return info
    
    print(f"🔄 @{username}: Apify failed, trying instagram120...", flush=True)
    info = await _check_via_instagram120(username)
    if info is not None:
        print(f"✅ @{username} found via instagram120", flush=True)
        return info
    
    print(f"❌ @{username}: NOT FOUND (banned/suspended)", flush=True)
    return None


# ---------- Browserless Screenshot Function ----------

async def get_browserless_screenshot(username: str):
    """Fetches a tightly cropped header screenshot of the Instagram profile."""
    if not BROWSERLESS_TOKEN or not INSTA_COOKIE:
        print("[browserless] Missing token or cookie in .env — will fallback.", flush=True)
        return None

    api_url = f"https://production-sfo.browserless.io/function?token={BROWSERLESS_TOKEN}"
    
    script = f"""
    export default async function ({{ page }}) {{
      await page.setCookie({{ name: 'sessionid', value: '{INSTA_COOKIE}', domain: '.instagram.com' }});
      await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
      await page.setViewport({{ width: 1280, height: 800 }});
      await page.emulateMediaFeatures([{{ name: 'prefers-color-scheme', value: 'dark' }}]);
      
      await page.goto("https://www.instagram.com/{username}/", {{ waitUntil: "networkidle2", timeout: 30000 }});
      
      // Wait for the profile picture to actually load to prevent blurring
      await page.waitForFunction(() => {{
          const img = document.querySelector('header img');
          return img && img.complete && img.naturalWidth > 0;
      }}, {{ timeout: 5000 }}).catch(() => console.log('Image load timeout'));

      await new Promise(r => setTimeout(r, 2000));
      
      // Dismiss popups
      await page.evaluate(() => {{
          const buttons = Array.from(document.querySelectorAll('button'));
          const notNowBtn = buttons.find(b => b.innerText && b.innerText.includes('Not now'));
          if (notNowBtn) {{ notNowBtn.click(); }} 
          else {{ const dialog = document.querySelector('div[role="dialog"]'); if (dialog) dialog.remove(); }}
      }});
      
      await new Promise(r => setTimeout(r, 2000));
      
      // 🔥 THE FIX: Target ONLY the <header> tag.
      const headerElement = await page.$('header');
      let screenshot;
      
      if (headerElement) {{
          // This takes a snapshot of exactly the profile info box
          screenshot = await headerElement.screenshot();
      }} else {{
          // Fallback just in case Instagram changes their HTML
          screenshot = await page.screenshot({{ clip: {{ x: 335, y: 0, width: 945, height: 300 }} }});
      }}
      
      return {{ data: screenshot.toString("base64"), type: "image/png" }};
    }}
    """
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, 
                data=script, 
                headers={"Content-Type": "application/javascript"},
                timeout=aiohttp.ClientTimeout(total=90)
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    image_string = result.get("data", "")
                    
                    if "," in image_string:
                        image_bytes = bytes([int(x) for x in image_string.split(",")])
                    else:
                        image_string = "".join(image_string.split())
                        image_string += "=" * ((4 - len(image_string) % 4) % 4)
                        image_bytes = base64.b64decode(image_string, validate=False)
                        
                    return discord.File(io.BytesIO(image_bytes), filename="profile.png")
                else:
                    print(f"[browserless] HTTP Error {resp.status}: {await resp.text()}", flush=True)
                    return None
    except Exception as e:
        print(f"[browserless] Exception: {e}", flush=True)
        return None


# ---------- timer UI ----------

def format_elapsed(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def format_elapsed_long(start_iso: str) -> str:
    start = datetime.fromisoformat(start_iso)
    delta = datetime.now(timezone.utc) - start
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} hours, {minutes} minutes, {seconds} seconds"


def build_screenshot_recovery_embed(info: dict, start_iso: str) -> discord.Embed:
    """Format for when the screenshot succeeds."""
    embed = discord.Embed(
        title=f"🎉 Account Recovered | @{info['username']} 🏆✅",
        color=discord.Color.green(),
        url=f"https://instagram.com/{info['username']}",
    )
    embed.description = (
        f"**Followers:** {info['followers']:,} | **Following:** {info['following']:,}\n"
        f"⏱️ **Time taken:** {format_elapsed_long(start_iso)}"
    )
    return embed


def build_fallback_recovery_embed(info: dict, start_iso: str) -> discord.Embed:
    """Original format for when the screenshot fails."""
    embed = discord.Embed(
        title="🎉 Account Recovered!",
        description=(
            f"**@{info['username']}** is back! 🏆✅\n\n"
            f"📊 **Stats:**\n"
            f"• Followers: {info['followers']:,}\n"
            f"• Following: {info['following']:,}\n"
            f"• Posts: {info['posts']:,}\n"
            f"• Verified: {'✅' if info['is_verified'] else '❌'}\n\n"
            f"⏱️ **Time taken:** {format_elapsed_long(start_iso)}"
        ),
        color=discord.Color.green(),
        url=f"https://instagram.com/{info['username']}",
    )
    if info.get('profile_pic_url'):
        embed.set_thumbnail(url=info['profile_pic_url'])
    return embed


def build_ban_embed(username: str, start_iso: str) -> discord.Embed:
    embed = discord.Embed(
        title="🚫 Account Banned!",
        description=(
            f"**@{username}** has been banned! 🚫❌\n\n"
            f"⏱️ **Banned after:** {format_elapsed_long(start_iso)}\n"
            f"🔗 https://instagram.com/{username}"
        ),
        color=discord.Color.red(),
    )
    return embed


# ---------- background loop ----------

@tasks.loop(minutes=CHECK_INTERVAL_MINUTES)
async def check_tracked_accounts():
    try:
        tracked = await api_get_tracked()
        if not tracked:
            print("check_tracked_accounts: nothing tracked, skipping.", flush=True)
            return

        config = await api_get_config()
        channel_id = config.get("notify_channel_id")
        if not channel_id:
            print("check_tracked_accounts: no notify channel set, skipping.", flush=True)
            return

        channel = bot.get_channel(int(channel_id))
        if channel is None:
            try:
                channel = await bot.fetch_channel(int(channel_id))
            except discord.HTTPException as e:
                print(f"check_tracked_accounts: could not fetch channel {channel_id}: {e}", flush=True)
                return

        pending_count = 0
        for username, meta in tracked.items():
            track_type = meta.get("track_type", "recovery")
            if track_type == "ban" and meta.get("banned", False):
                continue
            if track_type == "recovery" and meta.get("recovered", False):
                continue
            pending_count += 1
        
        print(f"🔍 Checking {pending_count} accounts...", flush=True)

        for username, meta in tracked.items():
            track_type = meta.get("track_type", "recovery")

            if track_type == "ban" and meta.get("banned", False):
                print(f"⏭️ @{username} already marked banned, skipping.", flush=True)
                continue
            if track_type == "recovery" and meta.get("recovered", False):
                print(f"⏭️ @{username} already marked recovered, skipping.", flush=True)
                continue

            print(f"🔍 Checking @{username} ({track_type})...", flush=True)
            info = await check_instagram_status(username)

            # ---------- BAN MONITORING ----------
            if track_type == "ban":
                if info is None:
                    fail_count = meta.get("fail_count", 0) + 1
                    print(f"📊 @{username} unreachable (check {fail_count}/2)", flush=True)
                    
                    if fail_count >= 2:
                        print(f"🚨 BAN CONFIRMED for @{username}!", flush=True)
                        embed = build_ban_embed(username, meta["start_time"])
                        
                        try:
                            await channel.send(embed=embed)
                            print(f"✅ Ban notification SENT for @{username}", flush=True)
                        except Exception as e:
                            print(f"❌ Failed to send ban notification: {e}", flush=True)
                        
                        ok = await api_mark_banned(username, datetime.now(timezone.utc).isoformat())
                        if ok:
                            print(f"✅ Banned notification sent for @{username}", flush=True)
                        else:
                            print(f"⚠️ Sent ban notification but DB save failed for @{username}", flush=True)
                    else:
                        await api_set_fail_count(username, fail_count)
                        print(f"⏳ @{username} unreachable (check {fail_count}/2) — waiting for confirmation.", flush=True)
                else:
                    if meta.get("fail_count", 0) > 0:
                        await api_set_fail_count(username, 0)
                        print(f"✅ @{username} is reachable again, reset fail_count to 0", flush=True)

            # ---------- RECOVERY MONITORING ----------
            elif track_type == "recovery":
                if info is not None and not meta.get("recovered", False):
                    print(f"🎉 RECOVERY DETECTED for @{username}!", flush=True)
                    
                    # Try to take the screenshot
                    file = await get_browserless_screenshot(username)
                    
                    try:
                        if file:
                            print(f"📸 Screenshot success! Sending new format...", flush=True)
                            embed = build_screenshot_recovery_embed(info, meta["start_time"])
                            embed.set_image(url="attachment://profile.png")
                            await channel.send(embed=embed, file=file)
                        else:
                            print(f"⚠️ Screenshot failed. Falling back to old format...", flush=True)
                            embed = build_fallback_recovery_embed(info, meta["start_time"])
                            await channel.send(embed=embed)
                            
                        print(f"✅ Recovery notification SENT for @{username}", flush=True)
                    except Exception as e:
                        print(f"❌ Failed to send recovery notification: {e}", flush=True)
                    
                    # Mark as recovered in database
                    ok = await api_mark_recovered(username, datetime.now(timezone.utc).isoformat())
                    if ok:
                        print(f"✅ Recovery notification sent for @{username}", flush=True)
                    else:
                        print(f"⚠️ Sent recovery notification but DB save failed for @{username}", flush=True)

    except Exception as e:
        print(f"check_tracked_accounts: UNHANDLED ERROR: {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()


@check_tracked_accounts.error
async def check_tracked_accounts_error(error):
    print(f"check_tracked_accounts: loop-level error: {type(error).__name__}: {error}", flush=True)


@check_tracked_accounts.before_loop
async def before_check():
    await bot.wait_until_ready()


# ---------- slash commands ----------

async def sync_commands_background():
    """Syncs slash commands in the background with a delay to avoid 429 Rate Limits."""
    await asyncio.sleep(5)  # Wait 5 seconds to let the bot stabilize
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced {len(synced)} slash commands.", flush=True)
    except discord.HTTPException as e:
        if e.status == 429:
            print("⚠️ Rate limited while syncing commands. Waiting 5 minutes...", flush=True)
            await asyncio.sleep(300)  # Wait 5 minutes before trying again
            try:
                synced = await bot.tree.sync()
                print(f"✅ Synced {len(synced)} slash commands after waiting.", flush=True)
            except Exception as retry_error:
                print(f"❌ Failed to sync commands after retry: {retry_error}", flush=True)
        else:
            print(f"❌ Failed to sync commands: {e}", flush=True)


@bot.event
async def on_ready():
    # 🚀 MOVE SYNC TO BACKGROUND TO PREVENT 429 ERRORS
    asyncio.create_task(sync_commands_background())
    
    check_tracked_accounts.start()
    print(f"Logged in as {bot.user} — checking every {CHECK_INTERVAL_MINUTES} min", flush=True)
    if not RAPIDAPI_KEY:
        print("WARNING: RAPIDAPI_KEY not set — checks will fail.", flush=True)
    if not APIFY_TOKEN:
        print("WARNING: APIFY_TOKEN not set — will skip straight to RapidAPI fallbacks.", flush=True)
    if not BROWSERLESS_TOKEN or not INSTA_COOKIE:
        print("WARNING: BROWSERLESS_TOKEN or INSTA_COOKIE not set — will always fallback to text-only.", flush=True)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        return
    print(f"[command error] /{interaction.command.name if interaction.command else '?'}: {error!r}", flush=True)
    message = f"⚠️ Something went wrong: `{error}`"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.tree.command(name="checknow", description="Debug: immediately check an account")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def checknow(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()
    await interaction.response.defer(thinking=True)
    info = await check_instagram_status(username)
    if info:
        await interaction.followup.send(
            f"✅ **@{info['username']}** is LIVE — "
            f"Followers: {info['followers']:,} | "
            f"Posts: {info['posts']:,} | "
            f"Verified: {'✅' if info['is_verified'] else '❌'}"
        )
    else:
        await interaction.followup.send(
            f"❌ **@{username}** is NOT reachable (banned/suspended/not found)."
        )


@bot.tree.command(name="track", description="Start tracking an Instagram account for unban recovery")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def track(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()

    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        await interaction.response.send_message(
            f"❌ `{username}` doesn't look like a valid Instagram username.",
            ephemeral=True,
        )
        return

    tracked = await api_get_tracked()
    if username in tracked and not tracked[username].get("recovered"):
        await interaction.response.send_message(f"⚠️ Already tracking @{username}.", ephemeral=True)
        return
    ok = await api_add_tracked(username, datetime.now(timezone.utc).isoformat(), track_type="recovery")
    if ok:
        await interaction.response.send_message(f"⏱️ Started tracking **@{username}**. I'll post here when it's back.")
    else:
        await interaction.response.send_message(
            f"❌ Failed to start tracking @{username} — database error.",
            ephemeral=True,
        )


@bot.tree.command(name="untrack", description="Stop tracking an Instagram account (recovery)")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def untrack(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()
    tracked = await api_get_tracked()
    if username in tracked:
        ok = await api_remove_tracked(username)
        if ok:
            await interaction.response.send_message(f"✅ Stopped tracking @{username}.")
        else:
            await interaction.response.send_message(
                f"❌ Failed to remove @{username} — database error.",
                ephemeral=True,
            )
    else:
        await interaction.response.send_message(f"❌ @{username} isn't being tracked.", ephemeral=True)


@bot.tree.command(name="ban", description="Track an Instagram account for ban detection")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def ban(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()

    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        await interaction.response.send_message(
            f"❌ `{username}` doesn't look like a valid Instagram username.",
            ephemeral=True,
        )
        return

    tracked = await api_get_tracked()
    if username in tracked and not tracked[username].get("banned"):
        await interaction.response.send_message(f"⚠️ Already monitoring @{username} for bans.", ephemeral=True)
        return
    ok = await api_add_tracked(username, datetime.now(timezone.utc).isoformat(), track_type="ban")
    if ok:
        await interaction.response.send_message(f"🚫 Started monitoring **@{username}** for bans. I'll post here if it goes down.")
    else:
        await interaction.response.send_message(
            f"❌ Failed to start monitoring @{username} — database error.",
            ephemeral=True,
        )


@bot.tree.command(name="unban", description="Stop monitoring an Instagram account for bans")
@app_commands.describe(username="Instagram username (without @)")
@is_owner()
async def unban(interaction: discord.Interaction, username: str):
    username = username.lstrip("@").strip()
    tracked = await api_get_tracked()
    if username in tracked:
        ok = await api_remove_tracked(username)
        if ok:
            await interaction.response.send_message(f"✅ Stopped monitoring @{username} for bans.")
        else:
            await interaction.response.send_message(
                f"❌ Failed to remove @{username} — database error.",
                ephemeral=True,
            )
    else:
        await interaction.response.send_message(f"❌ @{username} isn't being tracked.", ephemeral=True)


@bot.tree.command(name="list", description="List all Instagram accounts tracked for recovery")
@is_owner()
async def list_tracked(interaction: discord.Interaction):
    tracked = await api_get_tracked()
    recovery_only = {u: m for u, m in tracked.items() if m.get("track_type", "recovery") == "recovery"}
    if not recovery_only:
        await interaction.response.send_message("📭 Nothing is being tracked for recovery right now.")
        return

    pending = {u: m for u, m in recovery_only.items() if not m.get("recovered")}
    recovered = {u: m for u, m in recovery_only.items() if m.get("recovered")}

    lines = [
        "📊 **Tracked Accounts:**",
        f"Active: {len(pending)} | Recovered: {len(recovered)}",
        "─" * 32,
    ]

    if pending:
        lines.append("")
        lines.append("**Currently Tracking:**")
        for username, meta in pending.items():
            lines.append(f"`{username}` — ⏳ {format_elapsed(meta['start_time'])}")

    if recovered:
        lines.append("")
        lines.append("**Recovered:**")
        for username, meta in recovered.items():
            lines.append(f"`{username}` — ✅")

    await interaction.response.send_message("\n".join(lines))


@bot.tree.command(name="banlist", description="List all Instagram accounts monitored for bans")
@is_owner()
async def banlist(interaction: discord.Interaction):
    tracked = await api_get_tracked()
    ban_only = {u: m for u, m in tracked.items() if m.get("track_type", "recovery") == "ban"}
    if not ban_only:
        await interaction.response.send_message("📭 Nothing is being monitored for bans right now.")
        return

    active = {u: m for u, m in ban_only.items() if not m.get("banned")}
    banned = {u: m for u, m in ban_only.items() if m.get("banned")}

    lines = [
        "🚫 **Ban Monitoring:**",
        f"Active: {len(active)} | Banned: {len(banned)}",
        "─" * 32,
    ]

    if active:
        lines.append("")
        lines.append("**Currently Monitoring:**")
        for username, meta in active.items():
            lines.append(f"`{username}` — 📡 {format_elapsed(meta['start_time'])}")

    if banned:
        lines.append("")
        lines.append("**Banned:**")
        for username, meta in banned.items():
            lines.append(f"`{username}` — 🚫")

    await interaction.response.send_message("\n".join(lines))


@bot.tree.command(name="setchannel", description="Set this channel as the notification channel")
@is_owner()
async def setchannel(interaction: discord.Interaction):
    ok = await api_set_config("notify_channel_id", str(interaction.channel_id))
    if ok:
        config = await api_get_config()
        saved_id = config.get("notify_channel_id")
        if str(saved_id) == str(interaction.channel_id):
            await interaction.response.send_message(
                f"✅ Notifications will be posted in {interaction.channel.mention}."
            )
        else:
            await interaction.response.send_message(
                f"⚠️ Save request succeeded but database shows different value.",
                ephemeral=True,
            )
    else:
        await interaction.response.send_message(
            "❌ Failed to save the channel — database error.",
            ephemeral=True,
        )


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN not set.")
    if not D1_WORKER_URL or not D1_API_KEY:
        raise SystemExit("D1_WORKER_URL and D1_API_KEY must be set.")
    if not RAPIDAPI_KEY:
        raise SystemExit("RAPIDAPI_KEY not set.")
    start_keep_alive()
    bot.run(DISCORD_TOKEN)