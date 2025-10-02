import discord
import os
import json
import sqlite3
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Annotated

# --- Configuration Loading ---
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

with open('config.json', 'r') as f:
    config = json.load(f)
    
RANKED_ALLOWANCES = config['weekly_allowances']
UNRANKED_ALLOWANCE = config['unranked_allowance']
TIERS = {
    "7-Day Sprint": { "duration_days": 7, "founder_equity_pct": 0.50, "listing_fee": 25 },
    "30-Day Standard": { "duration_days": 30, "founder_equity_pct": 0.60, "listing_fee": 50 },
    "90-Day Marathon": { "duration_days": 90, "founder_equity_pct": 0.70, "listing_fee": 75 }
}
TRADING_FEE_PCT = 0.01
TRADING_CHANNEL_ID = int(config['trading_channel_id'])

# --- Bot Setup ---
intents = discord.Intents.default()
intents.members = True
bot = discord.Bot(intents=intents)

# --- Database Setup & Utility Functions ---
# (These are unchanged)
def setup_database():
    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, kcred_balance REAL DEFAULT 0, last_weekly_claim TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS goals (ticker TEXT PRIMARY KEY, founder_id INTEGER, scenario TEXT, target_score TEXT, tier TEXT, ico_price REAL, status TEXT, initial_deadline TEXT, current_deadline TEXT, extension_round INTEGER DEFAULT 0, FOREIGN KEY (founder_id) REFERENCES users (user_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS holdings (holding_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, ticker TEXT, amount INTEGER, asset_type TEXT DEFAULT 'TOKEN', FOREIGN KEY (user_id) REFERENCES users (user_id), FOREIGN KEY (ticker) REFERENCES goals (ticker))")
    cursor.execute("CREATE TABLE IF NOT EXISTS orders (order_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, ticker TEXT, order_type TEXT, amount INTEGER, price_per_token REAL, status TEXT DEFAULT 'OPEN', FOREIGN KEY (user_id) REFERENCES users (user_id), FOREIGN KEY (ticker) REFERENCES goals (ticker))")
    conn.commit()
    conn.close()

def get_user_balance(user_id):
    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("SELECT kcred_balance FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    if not result:
        cursor.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()
        return 0
    conn.close()
    return result[0]

# --- Bot Events ---
@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    setup_database()

# --- NEW AND IMPROVED /market COMMAND ---

@bot.slash_command(name="market", description="View all active goals on the exchange.")
async def market(ctx):
    await ctx.defer()
    conn = sqlite3.connect('economy.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Fetch all goals that are either in ICO or actively trading
    cursor.execute("SELECT * FROM goals WHERE status IN ('ICO', 'ACTIVE')")
    all_goals = cursor.fetchall()

    if not all_goals:
        await ctx.followup.send("The market is currently empty. Be the first to `/mint` a new goal!")
        conn.close()
        return

    embed = discord.Embed(title="📈 TSK Score Exchange Market", color=discord.Color.gold())
    
    for goal in all_goals:
        status = goal['status']
        ticker = goal['ticker']
        
        info_text = ""
        
        if status == 'ICO':
            cursor.execute("SELECT SUM(amount) FROM holdings WHERE ticker = ?", (ticker,))
            tokens_sold = cursor.fetchone()[0] or 0
            tokens_available = 100 - tokens_sold
            
            price = goal['ico_price']
            info_text = (
                f"**Price:** ✨ {price:,.2f} / token\n"
                f"**Available:** {tokens_available} tokens remaining"
            )
            
        elif status == 'ACTIVE':
            # Get lowest ask (sell order)
            cursor.execute("SELECT MIN(price_per_token) FROM orders WHERE ticker = ? AND order_type = 'SELL' AND status = 'OPEN'", (ticker,))
            lowest_ask = cursor.fetchone()[0]
            
            # Get highest bid (buy order)
            cursor.execute("SELECT MAX(price_per_token) FROM orders WHERE ticker = ? AND order_type = 'BUY' AND status = 'OPEN'", (ticker,))
            highest_bid = cursor.fetchone()[0]
            
            ask_text = f"✨ {lowest_ask:,.2f}" if lowest_ask else "N/A"
            bid_text = f"✨ {highest_bid:,.2f}" if highest_bid else "N/A"
            
            info_text = f"**Lowest Ask:** {ask_text}\n**Highest Bid:** {bid_text}"

        founder = await bot.fetch_user(goal['founder_id'])
        
        embed.add_field(
            name=f"`{ticker}` ({status}) - Founded by {founder.name}",
            value=(
                f"**Goal:** {goal['target_score']} in *{goal['scenario']}*\n"
                f"{info_text}\n"
                f"**Closes:** <t:{int(datetime.fromisoformat(goal['current_deadline']).timestamp())}:R>"
            ),
            inline=False
        )
    
    conn.close()
    embed.set_footer(text="Use /view <ticker> for more details or /orderbook <ticker> to see all orders.")
    await ctx.followup.send(embed=embed)


# --- All other commands remain the same ---
# (I will paste them all below for completeness)

@bot.slash_command(name="profile", description="Check a member's Aura balance, holdings, and open orders.")
async def profile(ctx, member: discord.Member = None):
    await ctx.defer() 
    target_member = member or ctx.author
    balance = get_user_balance(target_member.id)
    embed = discord.Embed(title=f"{target_member.display_name}'s Profile", color=discord.Color.blue()).set_thumbnail(url=target_member.display_avatar.url)
    embed.add_field(name="Aura Balance", value=f"✨ {balance:,.2f}", inline=False)
    conn = sqlite3.connect('economy.db'); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
    cursor.execute("SELECT ticker, amount FROM holdings WHERE user_id = ? AND asset_type = 'TOKEN' AND amount > 0", (target_member.id,))
    holdings = cursor.fetchall()
    if holdings:
        embed.add_field(name="Active Holdings", value="\n".join([f"`{h['ticker']}`: **{h['amount']}** tokens" for h in holdings]), inline=False)
    cursor.execute("SELECT order_id, ticker, order_type, amount, price_per_token FROM orders WHERE user_id = ? AND status = 'OPEN'", (target_member.id,))
    orders = cursor.fetchall()
    if orders:
        embed.add_field(name="Open Orders", value="\n".join([f"ID:`{o['order_id']}` {o['order_type']} `{o['ticker']}`: **{o['amount']}** @ ✨{o['price_per_token']:.2f}" for o in orders]), inline=False)
        embed.set_footer(text="Use /cancel_order <order_id> to cancel an order.")
    conn.close()
    await ctx.followup.send(embed=embed)


@bot.slash_command(name="weekly", description="Claim your weekly Aura allowance based on your highest rank.")
async def weekly(ctx):
    await ctx.defer(ephemeral=True)
    user_id = ctx.author.id
    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("SELECT last_weekly_claim FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    if result and result[0]:
        last_claim_time = datetime.fromisoformat(result[0])
        if datetime.utcnow() < last_claim_time + timedelta(days=7):
            time_left = (last_claim_time + timedelta(days=7)) - datetime.utcnow()
            days, rem = divmod(time_left.total_seconds(), 86400); hours, rem = divmod(rem, 3600); minutes, _ = divmod(rem, 60)
            await ctx.followup.send(f"You've already claimed your weekly allowance. Please wait {int(days)}d {int(hours)}h {int(minutes)}m.")
            conn.close()
            return
    allowance = UNRANKED_ALLOWANCE
    user_roles = [role.name for role in ctx.author.roles]
    for rank in RANKED_ALLOWANCES:
        if rank['role_name'] in user_roles: allowance = rank['amount']; break
    conn.execute("UPDATE users SET kcred_balance = kcred_balance + ?, last_weekly_claim = ? WHERE user_id = ?", (allowance, datetime.utcnow().isoformat(), user_id))
    conn.commit()
    conn.close()
    new_balance = get_user_balance(user_id)
    await ctx.followup.send(f"You have claimed your weekly allowance of **✨ {allowance:,.2f}**! Your new balance is **✨ {new_balance:,.2f}**.")


@bot.slash_command(name="mint", description="Mint a new goal token and start an ICO.")
async def mint(ctx,
    tier: Annotated[str, discord.Option(str, description="Choose the goal duration tier", choices=list(TIERS.keys()))],
    scenario: Annotated[str, discord.Option(str, description="The name of the KovaaK's scenario")],
    target_score: Annotated[str, discord.Option(str, description="Your target score for the scenario")],
    total_auras_to_raise: Annotated[float, discord.Option(float, description="The total amount of Auras you want to raise from investors")]
):
    await ctx.defer()
    founder_id = ctx.author.id
    tier_details = TIERS[tier]
    
    founder_balance = get_user_balance(founder_id)
    if founder_balance < tier_details['listing_fee']:
        await ctx.followup.send(f"You cannot afford the **✨ {tier_details['listing_fee']:,}** listing fee for this tier.", ephemeral=True)
        return

    scenario_abbr = "".join([word[0] for word in scenario.split()]).upper()
    ticker = f"{ctx.author.name.upper()[:4]}-{scenario_abbr}-{target_score}"

    deadline = datetime.utcnow() + timedelta(days=tier_details['duration_days'])
    founder_tokens = int(100 * tier_details['founder_equity_pct'])
    ico_tokens = 100 - founder_tokens

    if ico_tokens <= 0:
        await ctx.followup.send("This tier does not offer any tokens for public sale.", ephemeral=True)
        return
        
    ico_price = total_auras_to_raise / ico_tokens

    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    try:
        conn.execute("UPDATE users SET kcred_balance = kcred_balance - ? WHERE user_id = ?", (tier_details['listing_fee'], founder_id))
        cursor.execute("""
            INSERT INTO goals (ticker, founder_id, scenario, target_score, tier, ico_price, status, initial_deadline, current_deadline)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker, founder_id, scenario, target_score, tier, ico_price, 'ICO', deadline.isoformat(), deadline.isoformat()))
        cursor.execute("INSERT INTO holdings (user_id, ticker, amount) VALUES (?, ?, ?)", (founder_id, ticker, founder_tokens))
        conn.commit()
    except sqlite3.IntegrityError:
        await ctx.followup.send("A goal with a very similar name already exists. Please try a more unique name.", ephemeral=True)
        conn.close()
        return
    finally:
        conn.close()

    embed = discord.Embed(
        title="📢 New Initial Coin Offering (ICO)!",
        description=f"**{ctx.author.mention}** is undertaking a new challenge!",
        color=discord.Color.green()
    )
    embed.add_field(name="Ticker", value=f"`{ticker}`", inline=False)
    embed.add_field(name="Goal", value=f"Achieve a score of **{target_score}** in **{scenario}**.", inline=False)
    embed.add_field(name="Deadline", value=f"<t:{int(deadline.timestamp())}:R>", inline=False)
    embed.add_field(name="Tokens for Sale", value=f"{ico_tokens}", inline=True)
    embed.add_field(name="Total Funding Goal", value=f"✨ {total_auras_to_raise:,.2f}", inline=True)
    embed.add_field(name="Implied Price/Token", value=f"✨ {ico_price:,.2f}", inline=True)
    embed.set_footer(text=f"Use /buy_ico {ticker} <amount> to invest.")
    
    await ctx.followup.send(embed=embed)


@bot.slash_command(name="buy_ico", description="Buy tokens during an active ICO.")
async def buy_ico(ctx, 
    ticker: Annotated[str, discord.Option(str, description="The ticker symbol of the goal you want to invest in")],
    amount: Annotated[int, discord.Option(int, description="The number of tokens you want to buy")]
):
    await ctx.defer()
    investor_id = ctx.author.id
    ticker = ticker.upper()
    if amount <= 0:
        await ctx.followup.send("You must buy a positive number of tokens.", ephemeral=True)
        return

    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("SELECT founder_id, ico_price, status FROM goals WHERE ticker = ? AND status = 'ICO'", (ticker,))
    goal = cursor.fetchone()
    if not goal:
        await ctx.followup.send(f"No active ICO found for ticker `{ticker}`.", ephemeral=True)
        conn.close()
        return
        
    founder_id, ico_price, _ = goal
    if investor_id == founder_id:
        await ctx.followup.send("You cannot invest in your own ICO.", ephemeral=True)
        conn.close()
        return

    cursor.execute("SELECT SUM(amount) FROM holdings WHERE ticker = ?", (ticker,))
    tokens_owned = cursor.fetchone()[0] or 0
    tokens_for_sale = 100 - tokens_owned
    if amount > tokens_for_sale:
        await ctx.followup.send(f"There are only {tokens_for_sale} tokens left in this ICO. You cannot buy {amount}.", ephemeral=True)
        conn.close()
        return
        
    total_cost = amount * ico_price
    investor_balance = get_user_balance(investor_id)
    if investor_balance < total_cost:
        await ctx.followup.send(f"You cannot afford this purchase. You need **✨ {total_cost:,.2f}** but only have **✨ {investor_balance:,.2f}**.", ephemeral=True)
        conn.close()
        return

    try:
        conn.execute("UPDATE users SET kcred_balance = kcred_balance - ? WHERE user_id = ?", (total_cost, investor_id))
        conn.execute("UPDATE users SET kcred_balance = kcred_balance + ? WHERE user_id = ?", (total_cost, founder_id))
        cursor.execute("SELECT holding_id FROM holdings WHERE user_id = ? AND ticker = ?", (investor_id, ticker))
        existing_holding = cursor.fetchone()
        if existing_holding:
            cursor.execute("UPDATE holdings SET amount = amount + ? WHERE holding_id = ?", (amount, existing_holding[0]))
        else:
            cursor.execute("INSERT INTO holdings (user_id, ticker, amount) VALUES (?, ?, ?)", (investor_id, ticker, amount))
        conn.commit()
    except Exception as e:
        conn.rollback()
        await ctx.followup.send(f"An error occurred: {e}", ephemeral=True)
    finally:
        conn.close()

    await ctx.followup.send(f"**Success!** {ctx.author.mention} has purchased **{amount}** `{ticker}` tokens for **✨ {total_cost:,.2f}**.")


@bot.slash_command(name="sell", description="Place tokens for sale on the open market.")
async def sell(ctx, 
    ticker: Annotated[str, discord.Option(str, description="The ticker of the tokens you want to sell")],
    amount: Annotated[int, discord.Option(int, description="The number of tokens to sell")],
    price: Annotated[float, discord.Option(float, description="The price per token in Auras")]
):
    await ctx.defer()
    seller_id = ctx.author.id
    ticker = ticker.upper()

    if amount <= 0 or price <= 0:
        await ctx.followup.send("Amount and price must be positive numbers.", ephemeral=True)
        return

    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("SELECT amount FROM holdings WHERE user_id = ? AND ticker = ? AND asset_type = 'TOKEN'", (seller_id, ticker))
    holding = cursor.fetchone()
    if not holding or holding[0] < amount:
        await ctx.followup.send(f"You don't have enough `{ticker}` tokens to sell. You have {holding[0] if holding else 0}.", ephemeral=True)
        conn.close()
        return

    cursor.execute("SELECT status FROM goals WHERE ticker = ?", (ticker,))
    status_res = cursor.fetchone()
    
    if status_res and status_res[0] == 'ICO':
        cursor.execute("UPDATE goals SET status = 'ACTIVE' WHERE ticker = ?", (ticker,))
        conn.commit()
        cursor.execute("SELECT status FROM goals WHERE ticker = ?", (ticker,))
        status_res = cursor.fetchone()

    if not status_res or status_res[0] != 'ACTIVE':
        await ctx.followup.send(f"`{ticker}` is not currently available for open market trading. Its status is: `{status_res[0] if status_res else 'UNKNOWN'}`.", ephemeral=True)
        conn.close()
        return

    cursor.execute("UPDATE holdings SET amount = amount - ? WHERE user_id = ? AND ticker = ?", (amount, seller_id, ticker))
    cursor.execute("INSERT INTO orders (user_id, ticker, order_type, amount, price_per_token) VALUES (?, ?, ?, ?, ?)",
                   (seller_id, ticker, 'SELL', amount, price))
    conn.commit()
    conn.close()
    
    await ctx.followup.send(f"{ctx.author.mention} has placed **{amount}** `{ticker}` tokens for sale at **✨ {price:,.2f}** each.")
    await match_orders(bot, ticker)


@bot.slash_command(name="buy", description="Place a buy order for tokens on the open market.")
async def buy(ctx, 
    ticker: Annotated[str, discord.Option(str, description="The ticker of the tokens you want to buy")],
    amount: Annotated[int, discord.Option(int, description="The number of tokens to buy")],
    price: Annotated[float, discord.Option(float, description="The maximum price you're willing to pay per token")]
):
    await ctx.defer()
    buyer_id = ctx.author.id
    ticker = ticker.upper()

    if amount <= 0 or price <= 0:
        await ctx.followup.send("Amount and price must be positive numbers.", ephemeral=True)
        return
        
    total_cost = amount * price
    buyer_balance = get_user_balance(buyer_id)
    if buyer_balance < total_cost:
        await ctx.followup.send(f"You cannot afford this buy order. You need **✨ {total_cost:,.2f}** to cover the maximum cost, but only have **✨ {buyer_balance:,.2f}**.", ephemeral=True)
        return
        
    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM goals WHERE ticker = ?", (ticker,))
    status_res = cursor.fetchone()
    if not status_res or status_res[0] != 'ACTIVE':
        await ctx.followup.send(f"`{ticker}` is not currently available for open market trading. Its status is: `{status_res[0] if status_res else 'UNKNOWN'}`.", ephemeral=True)
        conn.close()
        return

    try:
        cursor.execute("INSERT INTO orders (user_id, ticker, order_type, amount, price_per_token) VALUES (?, ?, ?, ?, ?)",
                       (buyer_id, ticker, 'BUY', amount, price))
        conn.commit()
    finally:
        conn.close()

    await ctx.followup.send(f"{ctx.author.mention} has placed a buy order for **{amount}** `{ticker}` tokens at a max price of **✨ {price:,.2f}** each.")
    await match_orders(bot, ticker)


@bot.slash_command(name="view", description="View detailed information about a specific goal/token.")
async def view(ctx, ticker: str):
    await ctx.defer()
    ticker = ticker.upper()
    conn = sqlite3.connect('economy.db')
    cursor = conn.cursor()
    cursor.execute("SELECT founder_id, scenario, target_score, tier, ico_price, status, current_deadline FROM goals WHERE ticker = ?", (ticker,))
    goal = cursor.fetchone()
    conn.close()

    if not goal:
        await ctx.respond(f"No goal found with ticker `{ticker}`.", ephemeral=True)
        return
        
    founder_id, scenario, target_score, tier, ico_price, status, deadline_str = goal
    founder = await bot.fetch_user(founder_id)
    deadline = datetime.fromisoformat(deadline_str)

    embed = discord.Embed(
        title=f"Token Information: `{ticker}`",
        description=f"Founded by **{founder.name}**",
        color=discord.Color.dark_purple()
    )
    embed.add_field(name="Goal", value=f"**{target_score}** in *{scenario}*", inline=False)
    embed.add_field(name="Status", value=status, inline=True)
    embed.add_field(name="Tier", value=tier, inline=True)
    embed.add_field(name="Deadline", value=f"<t:{int(deadline.timestamp())}:R>", inline=True)
    embed.add_field(name="ICO Price", value=f"✨ {ico_price:,.2f}", inline=True)
    
    await ctx.followup.send(embed=embed)


@bot.slash_command(name="orderbook", description="View the current buy and sell orders for a token.")
async def orderbook(ctx, ticker: Annotated[str, discord.Option(str, description="The ticker of the market you want to view")]):
    await ctx.defer()
    ticker = ticker.upper()
    conn = sqlite3.connect('economy.db'); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
    cursor.execute("SELECT price_per_token, SUM(amount) as total_amount FROM orders WHERE ticker = ? AND order_type = 'SELL' AND status = 'OPEN' GROUP BY price_per_token ORDER BY price_per_token ASC", (ticker,))
    sell_orders = cursor.fetchall()
    cursor.execute("SELECT price_per_token, SUM(amount) as total_amount FROM orders WHERE ticker = ? AND order_type = 'BUY' AND status = 'OPEN' GROUP BY price_per_token ORDER BY price_per_token DESC", (ticker,))
    buy_orders = cursor.fetchall()
    conn.close()
    embed = discord.Embed(title=f"Order Book for `{ticker}`", color=discord.Color.orange())
    sell_text = "\n".join([f"✨ {s['price_per_token']:.2f} - **{s['total_amount']}** tokens" for s in sell_orders]) or "No open sell orders"
    embed.add_field(name="🔴 Asks (Sellers)", value=sell_text, inline=True)
    buy_text = "\n".join([f"✨ {b['price_per_token']:.2f} - **{b['total_amount']}** tokens" for b in buy_orders]) or "No open buy orders"
    embed.add_field(name="🟢 Bids (Buyers)", value=buy_text, inline=True)
    await ctx.followup.send(embed=embed)


@bot.slash_command(name="cancel_order", description="Cancel one of your open buy or sell orders.")
async def cancel_order(ctx, order_id: Annotated[int, discord.Option(int, description="The ID of the order you wish to cancel (from /profile)")]):
    await ctx.defer(ephemeral=True) 
    user_id = ctx.author.id
    conn = sqlite3.connect('economy.db'); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders WHERE order_id = ? AND user_id = ? AND status = 'OPEN'", (order_id, user_id))
    order = cursor.fetchone()
    if not order:
        await ctx.followup.send("Could not find an open order with that ID belonging to you.")
        conn.close()
        return
    if order['order_type'] == 'SELL':
        cursor.execute("UPDATE holdings SET amount = amount + ? WHERE user_id = ? AND ticker = ?", (order['amount'], user_id, order['ticker']))
    cursor.execute("UPDATE orders SET status = 'CANCELLED' WHERE order_id = ?", (order_id,))
    conn.commit()
    conn.close()
    await ctx.followup.send(f"Successfully cancelled order `{order_id}` ({order['order_type']} {order['amount']} {order['ticker']}).")


async def match_orders(bot: discord.Bot, ticker: str):
    print(f"Running full order matching engine for {ticker}...")
    conn = sqlite3.connect('economy.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    while True:
        cursor.execute("SELECT * FROM orders WHERE ticker = ? AND order_type = 'SELL' AND status = 'OPEN' AND amount > 0 ORDER BY price_per_token ASC, order_id ASC LIMIT 1", (ticker,))
        sell_order = cursor.fetchone()
        cursor.execute("SELECT * FROM orders WHERE ticker = ? AND order_type = 'BUY' AND status = 'OPEN' AND amount > 0 ORDER BY price_per_token DESC, order_id ASC LIMIT 1", (ticker,))
        buy_order = cursor.fetchone()

        if not sell_order or not buy_order or buy_order['price_per_token'] < sell_order['price_per_token']:
            break

        trade_price = sell_order['price_per_token'] if sell_order['order_id'] < buy_order['order_id'] else buy_order['price_per_token']
        trade_amount = min(buy_order['amount'], sell_order['amount'])
        total_value = trade_amount * trade_price
        commission = total_value * TRADING_FEE_PCT
        buyer_id = buy_order['user_id']
        seller_id = sell_order['user_id']
        
        buyer_balance = get_user_balance(buyer_id)
        if buyer_balance < total_value:
            cursor.execute("UPDATE orders SET status = 'CANCELLED' WHERE order_id = ?", (buy_order['order_id'],))
            conn.commit()
            print(f"Cancelled buy order {buy_order['order_id']} due to insufficient funds.")
            continue

        try:
            conn.execute("UPDATE users SET kcred_balance = kcred_balance - ? WHERE user_id = ?", (total_value, buyer_id))
            conn.execute("UPDATE users SET kcred_balance = kcred_balance + ? WHERE user_id = ?", (total_value - commission, seller_id))

            cursor.execute("SELECT holding_id FROM holdings WHERE user_id = ? AND ticker = ?", (buyer_id, ticker))
            buyer_holding = cursor.fetchone()
            if buyer_holding:
                cursor.execute("UPDATE holdings SET amount = amount + ? WHERE holding_id = ?", (trade_amount, buyer_holding['holding_id']))
            else:
                cursor.execute("INSERT INTO holdings (user_id, ticker, amount) VALUES (?, ?, ?)", (buyer_id, ticker, trade_amount))
            
            cursor.execute("UPDATE orders SET amount = amount - ? WHERE order_id = ?", (trade_amount, buy_order['order_id']))
            cursor.execute("UPDATE orders SET amount = amount - ? WHERE order_id = ?", (trade_amount, sell_order['order_id']))
            cursor.execute("UPDATE orders SET status = 'CLOSED' WHERE amount <= 0")
            conn.commit()

            print(f"Trade executed: {trade_amount} of {ticker} at {trade_price} each.")
            channel = bot.get_channel(TRADING_CHANNEL_ID)
            if channel:
                buyer = await bot.fetch_user(buyer_id)
                seller = await bot.fetch_user(seller_id)
                embed = discord.Embed(title="📈 Trade Executed!", description=f"**{trade_amount}** shares of `{ticker}` traded.", color=discord.Color.magenta())
                embed.add_field(name="Price", value=f"✨ {trade_price:,.2f} / token", inline=False)
                embed.add_field(name="Total Value", value=f"✨ {total_value:,.2f}", inline=False)
                embed.set_footer(text=f"Buyer: {buyer.name} | Seller: {seller.name}")
                await channel.send(embed=embed)

        except Exception as e:
            conn.rollback()
            print(f"An error occurred during trade execution, transaction rolled back. Error: {e}")
            break

    conn.close()


# --- Run the Bot ---
bot.run(TOKEN)
