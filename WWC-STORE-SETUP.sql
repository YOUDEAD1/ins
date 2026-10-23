-- =====================================================================
-- SafwanTiger Shop Bot — initial schema
-- Run this in the Supabase SQL editor (or `supabase db push`).
--
-- All statements use `if not exists` so this file is safe to re-run.
-- If a previous attempt left a half-applied state, run the DROP block
-- below first (it's commented out — uncomment it once, run, then run
-- the rest).
-- =====================================================================

-- ---------- (Optional) clean slate — uncomment to drop everything ---
-- Run these by themselves first if a previous migration attempt left
-- a partial state behind:
--
--   drop view  if exists public.products_view cascade;
--   drop table if exists public.referrals      cascade;
--   drop table if exists public.announcements  cascade;
--   drop table if exists public.settings       cascade;
--   drop table if exists public.payment_methods cascade;
--   drop table if exists public.deposits       cascade;
--   drop table if exists public.orders         cascade;
--   drop table if exists public.products       cascade;
--   drop table if exists public.categories     cascade;
--   drop table if exists public.admins         cascade;
--   drop table if exists public.users          cascade;

-- ---------- USERS ----------
create table if not exists public.users (
    telegram_id     bigint primary key,
    username        text,
    first_name      text,
    last_name       text,
    language        text not null default 'en' check (language in ('en','ar','vi')),
    balance         numeric(14,2) not null default 0,
    stock_alert     boolean not null default true,
    announcements   boolean not null default true,
    ref_code        text unique,
    -- referred_by intentionally has no FK constraint — enforcement
    -- is handled at the application layer to avoid self-reference
    -- quirks in some SQL editors. Add it later if you want strict
    -- integrity:
    --   alter table public.users
    --     add constraint users_referred_by_fkey
    --     foreign key (referred_by) references public.users(telegram_id)
    --     on delete set null;
    referred_by     bigint,
    joined_at       timestamptz not null default now(),
    last_seen_at    timestamptz not null default now()
);

create index if not exists users_referred_by_idx on public.users(referred_by);

-- ---------- ADMINS ----------
create table if not exists public.admins (
    telegram_id  bigint primary key,
    username     text,
    added_at     timestamptz not null default now()
);

-- ---------- CATEGORIES ----------
create table if not exists public.categories (
    id          bigserial primary key,
    name        text not null,
    emoji       text,
    sort_order  int not null default 0,
    active      boolean not null default true,
    created_at  timestamptz not null default now()
);

-- ---------- PRODUCTS ----------
create table if not exists public.products (
    id           bigserial primary key,
    category_id  bigint references public.categories(id) on delete cascade,
    name         text not null,
    description  text,
    note         text,
    price        numeric(14,2) not null check (price >= 0),
    stock        int not null default 0 check (stock >= 0),
    warranty     text,
    emoji        text,
    active       boolean not null default true,
    created_at   timestamptz not null default now()
);
create index if not exists products_category_idx on public.products(category_id);
create index if not exists products_active_idx   on public.products(active);

-- ---------- ORDERS ----------
create table if not exists public.orders (
    id           bigserial primary key,
    user_id      bigint not null references public.users(telegram_id) on delete cascade,
    product_id   bigint references public.products(id) on delete set null,
    product_name text not null,
    qty          int not null check (qty > 0),
    unit_price   numeric(14,2) not null,
    total        numeric(14,2) not null,
    delivery     text,
    status       text not null default 'paid' check (status in ('paid','refunded','cancelled')),
    created_at   timestamptz not null default now()
);
create index if not exists orders_user_idx on public.orders(user_id, created_at desc);

-- ---------- DEPOSITS ----------
create table if not exists public.deposits (
    id          bigserial primary key,
    user_id     bigint not null references public.users(telegram_id) on delete cascade,
    method      text not null,
    amount      numeric(14,2) not null check (amount > 0),
    status      text not null default 'pending' check (status in ('pending','approved','rejected')),
    reference   text,
    note        text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);
create index if not exists deposits_user_idx   on public.deposits(user_id, created_at desc);
create index if not exists deposits_status_idx on public.deposits(status);

-- ---------- PAYMENT METHODS ----------
create table if not exists public.payment_methods (
    id            bigserial primary key,
    name          text not null,
    instructions  text not null,
    min_amount    numeric(14,2) not null default 1,
    active        boolean not null default true,
    sort_order    int not null default 0,
    created_at    timestamptz not null default now()
);

-- ---------- SETTINGS (key/value JSONB; admin-editable runtime config) ---
-- Keys are namespaced like:
--   text.welcome
--   text.shop.title
--   button.shop
--   color.in_stock        -> "blue" | "green"
--   color.out_of_stock    -> "red"
--   emoji.fire            -> { unicode: "🔥", custom_emoji_id: "5440..." }
create table if not exists public.settings (
    key         text primary key,
    value       jsonb not null,
    updated_by  bigint,
    updated_at  timestamptz not null default now()
);

-- ---------- ANNOUNCEMENTS ----------
create table if not exists public.announcements (
    id           bigserial primary key,
    body         text not null,
    sent_at      timestamptz,
    created_by   bigint,
    created_at   timestamptz not null default now()
);

-- ---------- REFERRALS (audit log) ----------
create table if not exists public.referrals (
    id           bigserial primary key,
    referrer_id  bigint not null references public.users(telegram_id) on delete cascade,
    referee_id   bigint not null references public.users(telegram_id) on delete cascade,
    created_at   timestamptz not null default now(),
    unique (referrer_id, referee_id)
);

-- ---------- VIEW: products + category name + in_stock flag ----------
drop view if exists public.products_view cascade;
create or replace view public.products_view as
    select
        p.*,
        c.name as category_name,
        case when p.stock > 0 then true else false end as in_stock
    from public.products p
    left join public.categories c on c.id = p.category_id;

-- =====================================================================
-- Row Level Security (defense in depth — bot uses service_role key
-- which bypasses RLS, but enable it for any future anon access).
-- =====================================================================
alter table public.users           enable row level security;
alter table public.admins          enable row level security;
alter table public.categories      enable row level security;
alter table public.products        enable row level security;
alter table public.orders          enable row level security;
alter table public.deposits        enable row level security;
alter table public.payment_methods enable row level security;
alter table public.settings        enable row level security;
alter table public.announcements   enable row level security;
alter table public.referrals       enable row level security;

-- =====================================================================
-- Seed: primary admin (replace with the real ID from your .env)
-- =====================================================================
-- Owner access is supplied through ADMIN_USER_ID; no third-party admin is seeded.

-- Default settings seeds
insert into public.settings (key, value) values
    ('color.in_stock',     '"green"'::jsonb),
    ('color.out_of_stock', '"red"'::jsonb),
    ('text.welcome',       '"Welcome to SafwanTiger Shop"'::jsonb),
    ('text.menu_button',   '"Main Menu"'::jsonb)
on conflict (key) do nothing;


-- 0002_binance_pay.sql
-- Adds an optional `provider` column to payment_methods so the topup
-- flow can recognise auto-approving providers like Binance Pay.

alter table public.payment_methods
    add column if not exists provider text not null default 'manual'
    check (provider in ('manual', 'binance_pay'));

-- Index used to look up deposits by their merchantTradeNo when a
-- Binance Pay webhook arrives.
create index if not exists deposits_reference_idx
    on public.deposits (reference);


-- =====================================================================
-- Per-user click-sound preferences.
--
-- click_sound       : master ON/OFF for the click-sound effect.
-- click_sound_off   : list of button keys the user has individually
--                     muted (e.g. {'shop','topup'}).
-- =====================================================================

alter table public.users
  add column if not exists click_sound boolean not null default true;

alter table public.users
  add column if not exists click_sound_off text[] not null default '{}';


-- =====================================================================
-- Remove the click-sound user preference columns.
-- The click-sound feature was removed; these columns are no longer
-- read or written by the bot, so drop them to keep the schema clean.
-- =====================================================================

alter table public.users drop column if exists click_sound;
alter table public.users drop column if exists click_sound_off;


-- =====================================================================
-- 0005_user_profile_fields.sql
-- Add profile fields shown on the Settings/Profile screen:
--   - email     : user-supplied contact email (optional)
--   - region    : human-readable country/region label (optional)
--   - timezone  : IANA timezone identifier (e.g. 'Asia/Karachi')
--   - status    : free-form status string ('started bot', 'verified', …)
--
-- All fields are nullable; existing rows continue to work unchanged.
-- =====================================================================

alter table public.users
    add column if not exists email     text,
    add column if not exists region    text,
    add column if not exists timezone  text,
    add column if not exists status    text;


-- =====================================================================
-- 0006_wallet_ledger.sql
-- Records every change to a user's wallet balance so the "My Deposits"
-- screen can show a Wallet Balance History (purchases / admin adjusts /
-- deposit credits).
--
-- Pre-existing balance changes are NOT back-filled.
-- =====================================================================

create table if not exists public.wallet_ledger (
    id          bigserial primary key,
    user_id     bigint not null references public.users(telegram_id) on delete cascade,
    type        text   not null,
    -- Signed amount: negative = debit, positive = credit.
    amount      numeric(14,2) not null,
    -- Free-form reference (e.g. 'order:42', 'pay:128…', 'admin_add_balance').
    reference   text,
    created_at  timestamptz not null default now()
);

create index if not exists wallet_ledger_user_idx
    on public.wallet_ledger(user_id, created_at desc);


-- =====================================================================
-- 0007_gift_codes.sql
-- Admin-issued gift codes that users can redeem from Settings →
-- Redeem Gift Code. Each code has a fixed USDT value, optional expiry,
-- and a per-user redemption limit (default 1). Owner can raise the
-- limit per code if they want a code reusable across users.
-- =====================================================================

create table if not exists public.gift_codes (
    code              text primary key,
    amount            numeric(14,2) not null,
    -- Maximum total redemptions across ALL users (null = unlimited).
    max_redemptions   integer,
    -- Per-user redemption cap. 1 = each user can redeem once.
    per_user_limit    integer not null default 1,
    -- Optional expiry; null = no expiry.
    expires_at        timestamptz,
    note              text,
    created_by        bigint,
    created_at        timestamptz not null default now()
);

create table if not exists public.gift_code_redemptions (
    id          bigserial primary key,
    code        text not null references public.gift_codes(code) on delete cascade,
    user_id     bigint not null references public.users(telegram_id) on delete cascade,
    amount      numeric(14,2) not null,
    redeemed_at timestamptz not null default now()
);

create index if not exists gift_redemptions_user_idx
    on public.gift_code_redemptions(user_id);
create index if not exists gift_redemptions_code_idx
    on public.gift_code_redemptions(code);


-- =====================================================================
-- 0008_wallet_alert.sql
-- Add a third notification toggle so users can independently enable
-- wallet-related alerts (deposits, ledger entries, low-balance, …)
-- alongside Stock Alerts and Info Alerts.
--
-- Existing rows default to ON to preserve current behaviour.
-- =====================================================================

alter table public.users
    add column if not exists wallet_alert boolean not null default true;


-- =====================================================================
-- 0009_referral_earnings.sql
-- Referral-earning columns surfaced on the Refer & Earn screen.
--
-- Each referrer accumulates 1 % of every top-up made by users they
-- referred (capped at $1 per top-up — enforced in application code).
-- Earnings start in `available`. The user can transfer them to their
-- wallet at any time, or cash them out via support (≥ $1.00).
--
-- Columns:
--   referral_earned_total  - lifetime total credited (never decreases)
--   referral_available     - balance still claimable
--   referral_transferred   - moved to wallet
--   referral_withdrawn     - cashed out via support
-- =====================================================================

alter table public.users
    add column if not exists referral_earned_total numeric(14,2) not null default 0,
    add column if not exists referral_available    numeric(14,2) not null default 0,
    add column if not exists referral_transferred  numeric(14,2) not null default 0,
    add column if not exists referral_withdrawn    numeric(14,2) not null default 0;


-- =====================================================================
-- 0010_products_sort_order.sql
-- Manual product reordering for the admin product-management screen.
--
-- Mirrors the `sort_order` column already on `categories` and
-- `payment_methods`. Defaulted to 0 so legacy rows keep their
-- existing relative order (the queries break ties on `id ASC`,
-- preserving the historic insertion-order behaviour). Admin UI now
-- exposes ↑ / ↓ "move up" / "move down" buttons next to each
-- product row that swap the `sort_order` of two adjacent rows
-- across pages.
-- =====================================================================

alter table public.products
    add column if not exists sort_order int not null default 0;

create index if not exists products_sort_order_idx
    on public.products(sort_order, id);


-- =====================================================================
-- 0011_user_ban.sql
-- Lets the admin ban specific users so the bot ignores all their
-- updates (messages and inline-button taps) until they are unbanned.
--
--   - is_banned     : boolean flag, defaults to false. Existing users
--                     remain unbanned.
--   - banned_at     : when the most recent ban was applied (or NULL
--                     if never banned / currently unbanned).
--   - banned_reason : optional admin-supplied note shown nowhere to
--                     the banned user, only in the admin user card.
-- =====================================================================

alter table public.users
    add column if not exists is_banned     boolean not null default false,
    add column if not exists banned_at     timestamptz,
    add column if not exists banned_reason text;

create index if not exists users_is_banned_idx on public.users(is_banned)
    where is_banned = true;


-- =====================================================================
-- 0011_user_price_overrides.sql
-- Per-user, per-product price overrides.
--
-- Keyed by `telegram_id` (NOT users.telegram_id FK) so the admin can
-- pre-set a custom price for a Telegram ID that has never `/start`-ed
-- the bot yet — the override applies the moment that user opens the
-- product page for the first time.
--
-- Columns:
--   - telegram_id : the Telegram user this override applies to.
--   - product_id  : FK to public.products. ON DELETE CASCADE so
--                   deleting a product also drops every per-user
--                   override that pointed at it.
--   - price       : numeric(14,2), same shape as products.price.
--                   `>= 0` is enforced to match the product table.
--   - created_at  : when the override was first created.
--   - updated_at  : refreshed on every UPSERT.
--   - created_by  : Telegram ID of the admin who set/last-updated it
--                   (audit trail; nullable for migrations).
-- =====================================================================

create table if not exists public.user_price_overrides (
    telegram_id  bigint not null,
    product_id   bigint not null references public.products(id) on delete cascade,
    price        numeric(14,2) not null check (price >= 0),
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now(),
    created_by   bigint,
    primary key (telegram_id, product_id)
);

create index if not exists user_price_overrides_telegram_idx
    on public.user_price_overrides(telegram_id);

create index if not exists user_price_overrides_product_idx
    on public.user_price_overrides(product_id);


-- =====================================================================
-- 0013_promos.sql
-- Quantity-threshold flat-USDT discount promos.
--
-- Each row is one promo with two optional scope keys:
--   - product_id  : nullable FK → products.id. NULL means the promo
--                   applies to ANY product. ON DELETE CASCADE so
--                   deleting a product also drops every promo that
--                   pointed at it.
--   - telegram_id : nullable. NULL means the promo applies to ANY
--                   user. NOT a FK on users.telegram_id so admins
--                   can pre-set a personalized promo for a user who
--                   hasn't `/start`-ed the bot yet.
--
-- Specificity hierarchy (most specific match wins at order time):
--   3) telegram_id IS NOT NULL AND product_id IS NOT NULL
--   2) telegram_id IS NOT NULL AND product_id IS NULL
--   1) telegram_id IS NULL     AND product_id IS NOT NULL
--   0) telegram_id IS NULL     AND product_id IS NULL          (default)
-- Within a tier, the promo with the largest `discount_amount` wins.
--
-- min_qty:         line qty must be ≥ this for the promo to apply.
-- discount_amount: flat USDT off the line total when qty ≥ min_qty;
--                  application-side logic clamps the actual applied
--                  amount to never exceed the line total.
-- active:          soft-disable so admins can pause without deleting.
--
-- Multiple promos at the same scope tier are allowed (e.g. "10+ → -$5"
-- and "25+ → -$15") — the matching code picks the best one for the
-- caller's qty.
-- =====================================================================

create table if not exists public.promos (
    id              bigserial primary key,
    product_id      bigint references public.products(id) on delete cascade,
    telegram_id     bigint,
    name            text,
    min_qty         int not null check (min_qty >= 1),
    discount_amount numeric(14,2) not null check (discount_amount >= 0),
    active          boolean not null default true,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    created_by      bigint
);

-- Hot-path filter: scoped lookup by (telegram_id, product_id) on
-- active rows. We use coalesce to keep NULL scope rows in the index.
create index if not exists promos_scope_idx
    on public.promos(coalesce(telegram_id, 0), coalesce(product_id, 0))
    where active;

create index if not exists promos_product_idx
    on public.promos(product_id) where active;

create index if not exists promos_user_idx
    on public.promos(telegram_id) where active;

-- Audit column on orders: how much discount was applied at the time
-- of purchase. Defaults to 0 for legacy orders. unit_price stays the
-- per-user effective price (pre-discount) and `total` is the actual
-- USDT charged so existing reports stay correct.
alter table public.orders
    add column if not exists discount numeric(14,2) not null default 0
    check (discount >= 0);


-- =====================================================================
-- 0014_orders_promo_id.sql
-- Stamp the matching promo (if any) on each new order. Lets the
-- admin promo overview show *exact* impact stats — count of orders
-- and total USDT discounted — instead of inferring from the
-- floating-point `discount` column alone (which collides whenever
-- two promos happen to share the same discount value).
--
-- Nullable + ON DELETE SET NULL: deleting a promo doesn't ripple
-- into order history; the order keeps its `discount` value but
-- the link is forgotten.
-- =====================================================================

alter table public.orders
    add column if not exists promo_id bigint
    references public.promos(id) on delete set null;

-- Partial index so impact-stats queries (`where promo_id = ?`) stay
-- fast without indexing the long historical tail of pre-promo orders.
create index if not exists orders_promo_idx
    on public.orders(promo_id) where promo_id is not null;


-- =====================================================================
-- 0015_premium_shop_extensions.sql
--
-- Premium-shop UX overhaul. Adds:
--   - Per-product premium emoji (custom_emoji_id) used on the catalog
--     row icon and the product-page header (pic 1: 🎬 youtube ...).
--   - Optional unlimited-stock flag so the catalog row can render
--     "(Stock: ∞)" for digital subscription products.
--   - View Note attachment fields — admin can upload a .txt (or any
--     document) that the bot resends when the buyer taps View Note,
--     matching the "Gemini ⚠️18m Warning ‼️.txt" UX in pic 2.
--   - Per-product Using Method tutorial (text + optional photo /
--     video / document attachment + optional URL). Surfaced as a
--     button under every Order Delivered message.
--   - product_items pool — admin pastes the actual delivered payload
--     (codes, links, account creds) one per line; the bot consumes
--     `qty` items off the top of the pool per purchase. Mirrors the
--     pic-3 "Items: <quoted block>" delivery card.
--   - orders.delivered_items — preserves what was actually delivered
--     so it can be re-shown later (My Orders detail) without
--     re-consuming the pool.
--   - email_nag fields on users — track when we last sent the
--     12-hour "Please add your verified email" reminder so we don't
--     spam every interaction. The matching `email_nag_disabled` flag
--     mirrors the new "Email Reports" notifications toggle.
-- =====================================================================

alter table public.products
    add column if not exists emoji_id           text,
    add column if not exists note_file_id       text,
    add column if not exists note_file_name     text,
    add column if not exists note_file_mime     text,
    add column if not exists tutorial_text      text,
    add column if not exists tutorial_file_id   text,
    add column if not exists tutorial_file_type text,
    add column if not exists tutorial_url       text,
    add column if not exists unlimited_stock    boolean not null default false;

create table if not exists public.product_items (
    id                bigserial primary key,
    product_id        bigint not null references public.products(id) on delete cascade,
    payload           text not null,
    consumed_at       timestamptz,
    consumed_order_id bigint,
    created_at        timestamptz not null default now()
);
create index if not exists product_items_pool
    on public.product_items(product_id, id)
    where consumed_at is null;

alter table public.orders
    add column if not exists delivered_items text;

alter table public.users
    add column if not exists email_nag_disabled boolean not null default false,
    add column if not exists last_email_nag_at  timestamptz;

-- Default settings seeds for the new global tutorial + price-list
-- promo footer. Admin overrides these from the Bot Settings menu.
insert into public.settings (key, value) values
    ('bot_tutorial.text',          'null'::jsonb),
    ('bot_tutorial.file_id',       'null'::jsonb),
    ('bot_tutorial.file_type',     'null'::jsonb),
    ('bot_tutorial.url',           'null'::jsonb),
    ('price_list.promo_text',      'null'::jsonb)
on conflict (key) do nothing;

alter table public.product_items enable row level security;


-- 0016_payment_auto_verify.sql
--
-- Automatic payment verification for two new providers:
--
--   * usdt_trc20  – on-chain USDT on TRON  (verified via TronGrid)
--   * usdt_bep20  – on-chain USDT on BSC   (verified via a public BSC RPC)
--
-- Plus we keep `binance_pay` and add a Binance Pay queryOrder
-- auto-verification path on top of the existing webhook listener.
--
-- Schema changes:
--   * payment_methods.provider  – widen the CHECK to accept the two
--                                 new chain providers.
--   * payment_methods.address   – wallet address users send funds to.
--                                 Required for the chain providers,
--                                 ignored for `manual` and `binance_pay`.
--   * deposits.tx_hash          – on-chain transaction hash submitted
--                                 by the user (or merchantTradeNo for
--                                 Binance Pay). Indexed + de-duped so
--                                 the same tx can never credit twice.

-- 1. Widen the provider CHECK constraint --------------------------------
alter table public.payment_methods
    drop constraint if exists payment_methods_provider_check;

alter table public.payment_methods
    add constraint payment_methods_provider_check
    check (provider in ('manual', 'binance_pay', 'usdt_trc20', 'usdt_bep20'));

-- 2. Wallet address column ---------------------------------------------
alter table public.payment_methods
    add column if not exists address text;

-- 3. Per-deposit transaction hash + dedupe -----------------------------
alter table public.deposits
    add column if not exists tx_hash text;

create index if not exists deposits_tx_hash_idx
    on public.deposits (tx_hash);

-- A unique index is preferred but we allow null + repeated null (legacy
-- rows) by indexing only non-null values. Postgres treats duplicate
-- nulls as distinct in a regular UNIQUE INDEX — but to be explicit and
-- portable across editors we use a partial unique index.
create unique index if not exists deposits_tx_hash_uniq
    on public.deposits (tx_hash)
    where tx_hash is not null;


-- 0017_payment_auto_verify_v2.sql
--
-- Phase A of the rebuilt auto-verification flow. We re-introduce the
-- `binance_pay`, `usdt_trc20`, `usdt_bep20` providers (the columns
-- and indexes from migration 0016 still exist) and add two new ones:
--
--   * usdt_ton  – USDT Jetton on TON, verified via TonCenter REST API
--   * ltc       – Native Litecoin, verified via BlockCypher REST API
--
-- Schema changes:
--   * payment_methods.provider  – widen the CHECK to accept the two
--                                 new providers in addition to the
--                                 existing four.
--   * deposits.expected_amount  – locked LTC quote amount (in LTC),
--                                 used to validate the user-paid
--                                 amount against the rate locked at
--                                 deposit-creation time. Null for
--                                 every other provider.
--   * deposits.quote_expires_at – timestamp when the LTC rate quote
--                                 stops being valid. Null for every
--                                 other provider.

-- 1. Widen the provider CHECK constraint --------------------------------
alter table public.payment_methods
    drop constraint if exists payment_methods_provider_check;

alter table public.payment_methods
    add constraint payment_methods_provider_check
    check (provider in (
        'manual',
        'binance_pay',
        'usdt_trc20',
        'usdt_bep20',
        'usdt_ton',
        'ltc'
    ));

-- 2. LTC quote columns on deposits -------------------------------------
alter table public.deposits
    add column if not exists expected_amount numeric(20, 8);

alter table public.deposits
    add column if not exists quote_expires_at timestamptz;


-- 0018_deposits_order_intent.sql
--
-- Phase B of the auto-verify rebuild. Adds a single nullable JSONB
-- column to `deposits` that — when populated — turns a deposit into
-- a *direct-pay-per-order* payment instead of a wallet top-up.
--
-- The column shape is opaque to the DB but the bot expects:
--
--   {
--     "product_id": 12,
--     "product_name": "Netflix Premium",
--     "qty": 2,
--     "unit_price": 5.99,
--     "discount": 0,
--     "promo_id": null,
--     "total": 11.98
--   }
--
-- When the auto-verify orchestrator finds a deposit whose
-- `order_intent` is non-null, it runs the order-fulfilment path
-- (create order, decrement stock, claim items, deliver, send invoice)
-- instead of the legacy wallet-credit path. When `order_intent` is
-- null the deposit behaves exactly as before — the existing Phase A
-- top-up flow is untouched.

alter table public.deposits
    add column if not exists order_intent jsonb;


-- 0019_remove_binance_pay.sql
--
-- Strip Binance Pay from the payment-methods provider list.
--
-- The Binance Pay merchant API auto-verify path (introduced in 0002
-- and 0016/0017) is being retired because the merchant account is
-- region-blocked from `createOrder` / `queryOrder` (HTTP 451). A
-- fresh Binance Pay implementation will land in a follow-up
-- migration.
--
-- This migration:
--   1. Cancels any still-pending Binance Pay deposits so the dedupe
--      indexes on tx_hash / reference don't conflict with future rows.
--   2. Deletes the Binance Pay payment-method rows so the admin
--      panel never re-renders them.
--   3. Narrows the provider CHECK constraint to drop `binance_pay`.

-- 1. Cancel any pending Binance Pay deposits ---------------------------
-- The deposits table stores the method by its display name in
-- `method` (text), so we match on the names of any binance_pay rows
-- in payment_methods.
update public.deposits
set status = 'rejected',
    note = coalesce(note, '') ||
           case when note is null or note = '' then '' else E'\n' end ||
           '[binance_pay retired — auto-rejected by migration 0019]',
    updated_at = now()
where status = 'pending'
  and method in (
      select name from public.payment_methods where provider = 'binance_pay'
  );

-- 2. Delete the Binance Pay payment-method rows ------------------------
delete from public.payment_methods where provider = 'binance_pay';

-- 3. Narrow the provider CHECK constraint -----------------------------
alter table public.payment_methods
    drop constraint if exists payment_methods_provider_check;

alter table public.payment_methods
    add constraint payment_methods_provider_check
    check (provider in (
        'manual',
        'usdt_trc20',
        'usdt_bep20',
        'usdt_ton',
        'ltc'
    ));


-- 0020_binance_pay_restore.sql
--
-- Re-introduce Binance Pay as an auto-verifying payment provider, this
-- time using the personal-account Binance Spot API
-- (`GET /sapi/v1/pay/transactions`) instead of the merchant API. The
-- merchant API path was retired in 0019 because the merchant endpoints
-- (`createOrder` / `queryOrder`) returned HTTP 451 from every cloud
-- region we tried; the personal-account endpoint works through any VPN
-- exit IP that Binance allows.
--
-- Schema changes:
--   * payment_methods.provider — widen the CHECK to re-accept
--                                'binance_pay'.
--   * payment_methods.pay_name — NEW. Stores the human-readable
--                                Binance Pay Name (e.g. "urweebboii")
--                                rendered next to the Pay ID on the
--                                user-facing top-up screen. Null for
--                                every other provider.
--
-- The existing `address` column is reused to hold the merchant's
-- 10-digit Binance Pay ID (e.g. "1101801594") for binance_pay rows.
-- The verifier compares this against `receiverInfo.binanceId` of the
-- matched Pay transaction.

-- 1. Widen the provider CHECK constraint -------------------------------
alter table public.payment_methods
    drop constraint if exists payment_methods_provider_check;

alter table public.payment_methods
    add constraint payment_methods_provider_check
    check (provider in (
        'manual',
        'binance_pay',
        'usdt_trc20',
        'usdt_bep20',
        'usdt_ton',
        'ltc'
    ));

-- 2. New pay_name column ----------------------------------------------
alter table public.payment_methods
    add column if not exists pay_name text;


-- 0021_payment_methods_chrome.sql
--
-- Add per-payment-method "chrome" (color + premium emoji icon) so the
-- bot owner can style each payment-method button on the new Top-Up
-- Wallet / Select Payment Method screens individually. Stored on the
-- payment_methods row itself (rather than the generic settings table)
-- because the values are tied to a specific row's lifetime — when an
-- admin deletes a payment method, the customisation goes with it.
--
-- Columns:
--   * color_mode — one of 'none' | 'blue' | 'green' | 'red' | 'yellow'.
--                  Maps to the Bot API 9.4 button `style` (primary /
--                  success / danger / app-default). Defaults to 'none'
--                  so existing rows keep their current look.
--   * emoji_unicode — fallback unicode glyph rendered on non-premium
--                  Telegram clients (e.g. '🟡', '💎').
--   * emoji_id — Telegram premium custom_emoji_id rendered as the
--                  button icon for premium users (Bot API 9.4
--                  icon_custom_emoji_id). Null falls back to the
--                  generic provider glyph.

alter table public.payment_methods
    add column if not exists color_mode text not null default 'none'
        check (color_mode in ('none', 'blue', 'green', 'red', 'yellow'));

alter table public.payment_methods
    add column if not exists emoji_unicode text;

alter table public.payment_methods
    add column if not exists emoji_id text;


-- =====================================================================
-- 0022_admin_id_swap.sql
-- Swap the primary live-support admin telegram_id from the legacy
-- account (7913962419) to the new account (8004955979). The original
-- 0001_init.sql seed only inserts on a fresh database, so existing
-- deployments need this dedicated migration to actually flip the
-- `admins` row that powers `isAdmin()` (live support relay, admin
-- panel access, ban/unban, log-channel fallback DM, etc.).
--
-- Idempotent: insert-then-delete so re-running is a no-op once the
-- new admin is in place. No foreign keys reference `admins.telegram_id`
-- so the delete is safe.
-- =====================================================================

-- Owner access is supplied through ADMIN_USER_ID; no third-party admin is seeded.

-- Existing admin rows are retained.


-- =====================================================================
-- 0023_promo_user_exclusions.sql
-- Per-promo user exclusion list. Lets the admin keep a default
-- (or per-product) promo running for everyone *except* a specific
-- set of users — e.g. promo abusers, competitors, or anyone the
-- bot owner wants to opt out individually.
--
-- The exclusion is checked at resolve time AFTER the existing
-- scope filter:
--   1) the user's telegram_id matches the promo's scope, AND
--   2) the user's telegram_id is NOT in `excluded_telegram_ids`.
--
-- Defaults to an empty array so every existing promo row keeps
-- behaving exactly as it did before this migration.
-- =====================================================================

alter table public.promos
    add column if not exists excluded_telegram_ids bigint[] not null default '{}';

-- GIN index on the exclusion array. Cheap to maintain (most rows
-- stay empty) and lets the resolver short-circuit at query time
-- once we start filtering on it via the supabase-js `.contains` /
-- `.overlaps` operators.
create index if not exists promos_excluded_idx
    on public.promos using gin (excluded_telegram_ids);


-- =====================================================================
-- 0024_product_delivery_form.sql
--
-- Per-product post-purchase delivery form. For products where the
-- buyer has to submit their own details (email + password / code /
-- gift-card key / anything else) after paying, the admin can:
--   • Flip `delivery_form_enabled` to ON.
--   • Set an instruction message shown before the submission box.
--   • Declare a list of fields the buyer has to fill in.
--   • Set a success message shown when the buyer submits the form.
--   • Pick a vendor (Telegram chat id) the bot auto-DMs with the
--     submitted details + an order tag, every time the form is
--     submitted or resubmitted.
--
-- Submissions are stored 1:1 with `orders` so the buyer can tap
-- "Edit Details" later and resend a corrected version — we bump
-- `revision` and ping the vendor again with a "Resubmitted as
-- Corrected" header.
-- =====================================================================

alter table public.products
    add column if not exists delivery_form_enabled    boolean not null default false,
    add column if not exists delivery_instruction     text,
    add column if not exists delivery_fields          jsonb   not null default '[]'::jsonb,
    add column if not exists delivery_success_message text,
    add column if not exists delivery_vendor_chat_id  bigint,
    add column if not exists delivery_vendor_label    text;

create table if not exists public.order_delivery_submissions (
    id            bigserial primary key,
    order_id      bigint not null references public.orders(id) on delete cascade,
    user_id       bigint not null,
    product_id    bigint not null references public.products(id) on delete cascade,
    payload       jsonb  not null,
    revision      int    not null default 1,
    submitted_at  timestamptz not null default now(),
    updated_at    timestamptz not null default now(),
    unique(order_id)
);

create index if not exists order_delivery_submissions_user
    on public.order_delivery_submissions(user_id);
create index if not exists order_delivery_submissions_product
    on public.order_delivery_submissions(product_id);

alter table public.order_delivery_submissions enable row level security;


-- =====================================================================
-- 0025_product_pinning_and_oos_reorder.sql
--
-- Two related shop-UX features the bot owner asked for:
--
--   1. **Pinning / freeze position.** When `is_pinned = true` the
--      product is exempt from any automatic sort-order tweaks the
--      app might perform — most importantly it does NOT auto-slide
--      to the bottom of the catalog when it runs out of stock. It
--      stays exactly where the admin put it via the manual ↑ / ↓ /
--      ⏫ Top / ⏬ Bottom buttons.
--
--   2. **Auto-reorder on out-of-stock.** When an unpinned product's
--      `stock` transitions to 0 the app stashes the current
--      `sort_order` in `stashed_sort_order` and slams `sort_order` to
--      a sentinel value (`1_000_000_000`) so the catalog list pushes
--      it to the end. When the product is restocked (stock > 0
--      again) the original sort_order is restored from
--      `stashed_sort_order` and the stash is cleared, so the product
--      pops right back to where it used to live.
--
-- Both columns are nullable / default-safe so existing rows pick up
-- the new feature without any data backfill — `is_pinned` defaults
-- to false (no behaviour change) and `stashed_sort_order` defaults
-- to null (meaning "not currently auto-moved").
-- =====================================================================

alter table public.products
    add column if not exists is_pinned          boolean not null default false,
    add column if not exists stashed_sort_order int;

-- The catalog read query orders by `sort_order ASC, id ASC` and we
-- already have an index covering that tuple from migration 0010 —
-- no new index is required for `is_pinned` because the OOS sentinel
-- value in `sort_order` is what does the actual catalog reshuffle.


-- =====================================================================
-- 0026_backfill_oos_sort_order.sql
--
-- One-shot backfill for the OOS auto-reorder feature introduced in
-- migration 0025.
--
-- The TypeScript `applyStockTransition` hook only fires when a
-- product's stock value *changes* across the zero boundary (i.e. when
-- the catalog actually sees the in-stock -> OOS transition at
-- runtime). Products that were already at stock 0 *before* the new
-- code was deployed never go through that transition, so the new
-- code has no opportunity to stash their sort_order and shove them
-- to the end of the catalog.
--
-- Result: the bot-owner ships the new code, restarts the bot, and
-- sees the new ⏫ / ⏬ / 📌 buttons in the admin product list — but
-- the products that were already out of stock are still sitting in
-- their original slots, mixed in with in-stock products. That's the
-- exact symptom we're patching here.
--
-- This migration walks every currently-OOS product that:
--   * is NOT pinned (admin's explicit "stay put"),
--   * is NOT marked `unlimited_stock` (those have no concept of
--     OOS),
--   * doesn't already have a `stashed_sort_order` (so re-running
--     this migration is a no-op),
--   * has a `sort_order` strictly below the OOS sentinel
--     `1_000_000_000` (defense-in-depth — if the row is already at
--     the sentinel it must have been auto-moved by 0025+code on a
--     real transition, leave it alone),
-- and stashes its current sort_order into `stashed_sort_order` then
-- slams `sort_order` to the same sentinel value the TS hook uses
-- (`OUT_OF_STOCK_SORT_ORDER = 1_000_000_000`). The catalog read
-- query orders by (sort_order ASC, id ASC) so the backfilled rows
-- naturally fall to the very end of the list.
--
-- On the next restock (admin uploads items / increments stock /
-- syncs the pool), the existing TS transition hook will see the
-- in-stock value, read the stash, and pop the product right back to
-- its old admin-set slot. No code changes required — this migration
-- is purely a data fix.
-- =====================================================================

update public.products
   set stashed_sort_order = sort_order,
       sort_order         = 1000000000
 where (stock is null or stock <= 0)
   and coalesce(unlimited_stock, false) = false
   and coalesce(is_pinned, false)       = false
   and stashed_sort_order is null
   and sort_order < 1000000000;


-- =====================================================================
-- 0027_referral_rewards.sql
-- Add per-product referral unlock requirements + redemption tracking.
-- =====================================================================

alter table public.products
    add column if not exists referral_required_count int not null default 0
        check (referral_required_count >= 0);

create table if not exists public.referral_redemptions (
    id           bigserial primary key,
    user_id      bigint not null references public.users(telegram_id) on delete cascade,
    product_id   bigint not null references public.products(id) on delete cascade,
    order_id     bigint references public.orders(id) on delete set null,
    redeemed_at  timestamptz not null default now(),
    unique (user_id, product_id)
);

create index if not exists referral_redemptions_user_idx
    on public.referral_redemptions(user_id);
create index if not exists referral_redemptions_product_idx
    on public.referral_redemptions(product_id);

alter table public.referral_redemptions enable row level security;


-- =====================================================================
-- 0028_clear_catalog_prices_promos.sql
--
-- NO-OP SAFETY MIGRATION.
--
-- The original version of this file deleted catalog products, promos,
-- product items, and per-user price overrides. It is intentionally kept
-- as a no-op so future deploys do not accidentally wipe live shop data.
-- =====================================================================

do $$
begin
    raise notice '0028_clear_catalog_prices_promos is disabled; no data was changed.';
end;
$$;


-- =====================================================================
-- 0029_ensure_referral_rewards_schema.sql
-- Safety net for Referral Pay.
--
-- Some live databases may have older migrations applied but still miss
-- the Referral Pay schema. This keeps the admin "Referral Pay"
-- button from failing when it saves products.referral_required_count.
-- =====================================================================

alter table public.products
    add column if not exists referral_required_count int not null default 0
        check (referral_required_count >= 0);

create table if not exists public.referral_redemptions (
    id           bigserial primary key,
    user_id      bigint not null references public.users(telegram_id) on delete cascade,
    product_id   bigint not null references public.products(id) on delete cascade,
    order_id     bigint references public.orders(id) on delete set null,
    redeemed_at  timestamptz not null default now(),
    unique (user_id, product_id)
);

create index if not exists referral_redemptions_user_idx
    on public.referral_redemptions(user_id);

create index if not exists referral_redemptions_product_idx
    on public.referral_redemptions(product_id);

alter table public.referral_redemptions enable row level security;


-- =====================================================================
-- 0030_referral_payment_balance.sql
--
-- Converts the old one-time "referral reward" table into a reusable
-- referral-payment ledger:
--   available referrals = invited users - referrals already spent
--
-- This migration is non-destructive. Existing redemption rows are
-- retained and backfilled with the referral cost that applied to the
-- original order where possible.
-- =====================================================================

alter table public.products
    add column if not exists referral_required_count int not null default 0
        check (referral_required_count >= 0);

create table if not exists public.referral_redemptions (
    id             bigserial primary key,
    user_id        bigint not null references public.users(telegram_id) on delete cascade,
    product_id     bigint not null references public.products(id) on delete cascade,
    order_id       bigint references public.orders(id) on delete set null,
    referral_cost  int not null default 0 check (referral_cost >= 0),
    redeemed_at    timestamptz not null default now()
);

alter table public.referral_redemptions
    add column if not exists referral_cost int not null default 0
        check (referral_cost >= 0);

-- The old model allowed one referral purchase per user/product.
-- Referral Pay is a balance, so repeat purchases must be allowed.
alter table public.referral_redemptions
    drop constraint if exists referral_redemptions_user_id_product_id_key;

drop index if exists public.referral_redemptions_user_id_product_id_key;

-- Preserve the referral cost of purchases made under the old model.
update public.referral_redemptions rr
   set referral_cost = greatest(
       coalesce((
           select p.referral_required_count
             from public.products p
            where p.id = rr.product_id
       ), 0)
       *
       coalesce((
           select o.qty
             from public.orders o
            where o.id = rr.order_id
       ), 1),
       0
   )
 where rr.referral_cost = 0;

create index if not exists referral_redemptions_user_idx
    on public.referral_redemptions(user_id);

create index if not exists referral_redemptions_product_idx
    on public.referral_redemptions(product_id);

create unique index if not exists referral_redemptions_order_unique_idx
    on public.referral_redemptions(order_id)
    where order_id is not null;

alter table public.referral_redemptions enable row level security;

-- Atomically verify and spend referral balance. The advisory lock
-- serializes simultaneous purchases by the same Telegram user.
create or replace function public.spend_referral_balance(
    p_user_id bigint,
    p_product_id bigint,
    p_order_id bigint,
    p_referral_cost int
)
returns table (
    total_referrals int,
    spent_referrals int,
    available_referrals int
)
language plpgsql
security definer
set search_path = public
as $$
declare
    v_total int;
    v_spent int;
begin
    if p_referral_cost <= 0 then
        raise exception 'INVALID_REFERRAL_COST';
    end if;

    perform pg_advisory_xact_lock(p_user_id);

    select count(*)::int
      into v_total
      from public.referrals
     where referrer_id = p_user_id;

    select coalesce(sum(referral_cost), 0)::int
      into v_spent
      from public.referral_redemptions
     where user_id = p_user_id;

    if (v_total - v_spent) < p_referral_cost then
        raise exception 'INSUFFICIENT_REFERRALS';
    end if;

    insert into public.referral_redemptions (
        user_id,
        product_id,
        order_id,
        referral_cost
    )
    values (
        p_user_id,
        p_product_id,
        p_order_id,
        p_referral_cost
    );

    return query
    select
        v_total,
        v_spent + p_referral_cost,
        v_total - v_spent - p_referral_cost;
end;
$$;

grant execute on function public.spend_referral_balance(bigint, bigint, bigint, int)
    to service_role;


-- =====================================================================
-- 0031_reconcile_product_stock_order.sql
--
-- Reconciles every existing product with the automatic catalog order:
-- out-of-stock products move to the bottom, and restocked products
-- return to their previously stashed position.
-- =====================================================================

alter table public.products
    add column if not exists is_pinned boolean not null default false,
    add column if not exists stashed_sort_order int;

-- Restore products that have stock again but are still sitting at the
-- synthetic out-of-stock sort position.
update public.products
   set sort_order = stashed_sort_order,
       stashed_sort_order = null
 where stock > 0
   and coalesce(unlimited_stock, false) = false
   and stashed_sort_order is not null;

-- Move all currently out-of-stock products to the bottom.
update public.products
   set stashed_sort_order = sort_order,
       sort_order = 1000000000
 where stock <= 0
   and coalesce(unlimited_stock, false) = false
   and stashed_sort_order is null
   and sort_order < 1000000000;


-- =====================================================================
-- 0032_referral_convert_to_wallet.sql
--
-- Lets users convert referral balance into wallet balance:
--   20 available refs = 1.00 USDT
--
-- Conversion spends the same referral balance used by Referral Pay, so
-- converted refs cannot also be used for product purchases.
-- =====================================================================

create table if not exists public.referral_conversions (
    id          bigserial primary key,
    user_id     bigint not null references public.users(telegram_id) on delete cascade,
    refs_spent  int not null check (refs_spent > 0),
    amount      numeric(14,2) not null check (amount > 0),
    created_at  timestamptz not null default now()
);

create index if not exists referral_conversions_user_idx
    on public.referral_conversions(user_id, created_at desc);

alter table public.referral_conversions enable row level security;

-- Update product Referral Pay spending to include refs already
-- converted into wallet balance.
create or replace function public.spend_referral_balance(
    p_user_id bigint,
    p_product_id bigint,
    p_order_id bigint,
    p_referral_cost int
)
returns table (
    total_referrals int,
    spent_referrals int,
    available_referrals int
)
language plpgsql
security definer
set search_path = public
as $$
declare
    v_total int;
    v_spent int;
begin
    if p_referral_cost <= 0 then
        raise exception 'INVALID_REFERRAL_COST';
    end if;

    perform pg_advisory_xact_lock(p_user_id);

    select count(*)::int
      into v_total
      from public.referrals
     where referrer_id = p_user_id;

    select
        coalesce((
            select sum(referral_cost)
              from public.referral_redemptions
             where user_id = p_user_id
        ), 0)::int
        +
        coalesce((
            select sum(refs_spent)
              from public.referral_conversions
             where user_id = p_user_id
        ), 0)::int
      into v_spent;

    if (v_total - v_spent) < p_referral_cost then
        raise exception 'INSUFFICIENT_REFERRALS';
    end if;

    insert into public.referral_redemptions (
        user_id,
        product_id,
        order_id,
        referral_cost
    )
    values (
        p_user_id,
        p_product_id,
        p_order_id,
        p_referral_cost
    );

    return query
    select
        v_total,
        v_spent + p_referral_cost,
        v_total - v_spent - p_referral_cost;
end;
$$;

create or replace function public.convert_referrals_to_wallet(
    p_user_id bigint,
    p_referral_cost int default 20,
    p_usdt_amount numeric default 1.00
)
returns table (
    total_referrals int,
    spent_referrals int,
    available_referrals int,
    converted_amount numeric,
    new_balance numeric
)
language plpgsql
security definer
set search_path = public
as $$
declare
    v_total int;
    v_spent int;
    v_new_balance numeric;
begin
    if p_referral_cost <= 0 or p_usdt_amount <= 0 then
        raise exception 'INVALID_REFERRAL_CONVERSION';
    end if;

    perform pg_advisory_xact_lock(p_user_id);

    select count(*)::int
      into v_total
      from public.referrals
     where referrer_id = p_user_id;

    select
        coalesce((
            select sum(referral_cost)
              from public.referral_redemptions
             where user_id = p_user_id
        ), 0)::int
        +
        coalesce((
            select sum(refs_spent)
              from public.referral_conversions
             where user_id = p_user_id
        ), 0)::int
      into v_spent;

    if (v_total - v_spent) < p_referral_cost then
        raise exception 'INSUFFICIENT_REFERRALS';
    end if;

    insert into public.referral_conversions (
        user_id,
        refs_spent,
        amount
    )
    values (
        p_user_id,
        p_referral_cost,
        p_usdt_amount
    );

    update public.users
       set balance = balance + p_usdt_amount
     where telegram_id = p_user_id
     returning balance into v_new_balance;

    insert into public.wallet_ledger (
        user_id,
        type,
        amount,
        reference
    )
    values (
        p_user_id,
        'referral_convert',
        p_usdt_amount,
        'referral_convert:' || p_referral_cost::text
    );

    return query
    select
        v_total,
        v_spent + p_referral_cost,
        v_total - v_spent - p_referral_cost,
        p_usdt_amount,
        v_new_balance;
end;
$$;

grant execute on function public.spend_referral_balance(bigint, bigint, bigint, int)
    to service_role;

grant execute on function public.convert_referrals_to_wallet(bigint, int, numeric)
    to service_role;


alter table public.users
  add column if not exists currency text not null default 'USDT';

update public.users
set currency = 'USDT'
where currency is null or trim(currency) = '';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'users_currency_code_check'
  ) then
    alter table public.users
      add constraint users_currency_code_check
      check (currency in (
        'USDT', 'USD', 'PKR', 'INR', 'BDT', 'AED', 'SAR', 'TRY',
        'IDR', 'PHP', 'VND', 'THB', 'MYR', 'SGD', 'EUR', 'GBP',
        'CAD', 'AUD', 'NGN', 'EGP'
      ));
  end if;
end $$;


-- 0035_bybit_pay_provider.sql
--
-- Add Bybit Pay / Bybit internal-transfer verification to the
-- payment method provider constraint.

alter table public.payment_methods
    drop constraint if exists payment_methods_provider_check;

alter table public.payment_methods
    add constraint payment_methods_provider_check
    check (provider in (
        'manual',
        'binance_pay',
        'bybit_pay',
        'usdt_trc20',
        'usdt_bep20',
        'usdt_ton',
        'ltc'
    ));


-- Reseller Product API
--
-- Users can generate an API key from Telegram and use it from their
-- own website/bot to list products, check wallet balance, and place
-- wallet-funded orders. Keys are stored as SHA-256 hashes only.

create table if not exists public.reseller_api_keys (
    id bigserial primary key,
    user_id bigint not null references public.users(telegram_id) on delete cascade,
    key_hash text not null unique,
    key_prefix text not null,
    active boolean not null default true,
    created_at timestamptz not null default now(),
    last_used_at timestamptz,
    revoked_at timestamptz
);

alter table public.reseller_api_keys add column if not exists active boolean not null default true;

create index if not exists reseller_api_keys_user_active_idx
    on public.reseller_api_keys(user_id, active);

create table if not exists public.reseller_api_orders (
    id bigserial primary key,
    user_id bigint not null references public.users(telegram_id) on delete cascade,
    api_key_id bigint references public.reseller_api_keys(id) on delete set null,
    order_id bigint not null references public.orders(id) on delete cascade,
    product_id integer not null references public.products(id) on delete restrict,
    qty integer not null check (qty > 0),
    total numeric not null check (total >= 0),
    request_id text,
    created_at timestamptz not null default now(),
    unique (user_id, request_id)
);

create index if not exists reseller_api_orders_user_created_idx
    on public.reseller_api_orders(user_id, created_at desc);



-- Upstream supplier APIs
--
-- These tables let the shop owner connect outside reseller/supplier
-- APIs, map selected local products to supplier product ids, and keep
-- a log of every automatic supplier order attempt.

create table if not exists public.supplier_api_sources (
    id bigserial primary key,
    name text not null,
    base_url text not null,
    api_key text not null default '',
    auth_mode text not null default 'x-api-key'
        check (auth_mode in ('none', 'bearer', 'x-api-key', 'query')),
    key_header text not null default 'x-api-key',
    key_query_param text not null default 'api_key',
    products_path text not null default '/products',
    balance_path text not null default '/balance',
    order_path text not null default '/order',
    order_method text not null default 'POST'
        check (order_method in ('GET', 'POST')),
    balance_json_path text not null default 'balance',
    products_json_path text not null default 'products',
    product_id_json_path text not null default 'id',
    product_name_json_path text not null default 'name',
    product_price_json_path text not null default 'price',
    product_stock_json_path text not null default 'stock',
    order_items_json_path text not null default 'items',
    order_status_json_path text not null default 'status',
    order_request_template jsonb not null default
        '{"product_id":"{{supplier_product_id}}","quantity":"{{qty}}","request_id":"{{request_id}}"}'::jsonb,
    enabled boolean not null default true,
    markup_percent numeric(8,2) not null default 25,
    fixed_markup numeric(12,4) not null default 0,
    low_balance_threshold numeric(12,4) not null default 5,
    notes text,
    last_balance numeric(12,4),
    last_sync_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists supplier_api_sources_enabled_idx
    on public.supplier_api_sources(enabled, created_at desc);

create table if not exists public.supplier_product_links (
    id bigserial primary key,
    local_product_id integer not null references public.products(id) on delete cascade,
    supplier_id bigint not null references public.supplier_api_sources(id) on delete cascade,
    supplier_product_id text not null,
    supplier_product_name text,
    supplier_cost numeric(12,4),
    supplier_stock integer,
    auto_order boolean not null default true,
    auto_sync_stock boolean not null default true,
    fallback_manual boolean not null default true,
    last_sync_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (local_product_id)
);

create index if not exists supplier_product_links_supplier_idx
    on public.supplier_product_links(supplier_id);

create index if not exists supplier_product_links_product_idx
    on public.supplier_product_links(local_product_id);

create table if not exists public.supplier_order_logs (
    id bigserial primary key,
    supplier_id bigint references public.supplier_api_sources(id) on delete set null,
    local_order_id bigint references public.orders(id) on delete cascade,
    local_product_id integer references public.products(id) on delete set null,
    supplier_product_id text,
    status text not null default 'pending'
        check (status in ('pending', 'success', 'failed', 'manual')),
    request_payload jsonb not null default '{}'::jsonb,
    response_payload jsonb not null default '{}'::jsonb,
    error text,
    created_at timestamptz not null default now()
);

create index if not exists supplier_order_logs_order_idx
    on public.supplier_order_logs(local_order_id);

create index if not exists supplier_order_logs_supplier_created_idx
    on public.supplier_order_logs(supplier_id, created_at desc);


-- Supplier API easy import controls
--
-- Adds button-driven supplier options on top of 0037:
-- - auto import newly seen supplier products during sync
-- - choose whether auto-imported products are visible immediately
-- - choose the local category name for imported supplier products

alter table public.supplier_api_sources
  add column if not exists auto_import_new_products boolean not null default false,
  add column if not exists auto_import_active boolean not null default false,
  add column if not exists import_category_name text;

update public.supplier_api_sources
set import_category_name = coalesce(import_category_name, 'Supplier - ' || name)
where import_category_name is null;


-- =====================================================================
-- 0039_referral_admin_adjustments.sql
--
-- Admin-controlled referral balance corrections.
-- This keeps real invite rows untouched:
--   effective referral total = real referrals + admin adjustments
--   available referrals = effective total - purchase spend - conversion spend
-- =====================================================================

create table if not exists public.referral_adjustments (
    id          bigserial primary key,
    user_id     bigint not null references public.users(telegram_id) on delete cascade,
    delta       int not null check (delta <> 0),
    reason      text,
    created_by  bigint,
    created_at  timestamptz not null default now()
);

create index if not exists referral_adjustments_user_idx
    on public.referral_adjustments(user_id, created_at desc);

alter table public.referral_adjustments enable row level security;

create or replace function public.spend_referral_balance(
    p_user_id bigint,
    p_product_id bigint,
    p_order_id bigint,
    p_referral_cost int
)
returns table (
    total_referrals int,
    spent_referrals int,
    available_referrals int
)
language plpgsql
security definer
set search_path = public
as $$
declare
    v_total int;
    v_spent int;
begin
    if p_referral_cost <= 0 then
        raise exception 'INVALID_REFERRAL_COST';
    end if;

    perform pg_advisory_xact_lock(p_user_id);

    select greatest(
        0,
        (
            select count(*)::int
              from public.referrals
             where referrer_id = p_user_id
        )
        +
        coalesce((
            select sum(delta)
              from public.referral_adjustments
             where user_id = p_user_id
        ), 0)::int
    )
      into v_total;

    select
        coalesce((
            select sum(referral_cost)
              from public.referral_redemptions
             where user_id = p_user_id
        ), 0)::int
        +
        coalesce((
            select sum(refs_spent)
              from public.referral_conversions
             where user_id = p_user_id
        ), 0)::int
      into v_spent;

    if (v_total - v_spent) < p_referral_cost then
        raise exception 'INSUFFICIENT_REFERRALS';
    end if;

    insert into public.referral_redemptions (
        user_id,
        product_id,
        order_id,
        referral_cost
    )
    values (
        p_user_id,
        p_product_id,
        p_order_id,
        p_referral_cost
    );

    return query
    select
        v_total,
        v_spent + p_referral_cost,
        v_total - v_spent - p_referral_cost;
end;
$$;

create or replace function public.convert_referrals_to_wallet(
    p_user_id bigint,
    p_referral_cost int default 20,
    p_usdt_amount numeric default 1.00
)
returns table (
    total_referrals int,
    spent_referrals int,
    available_referrals int,
    converted_amount numeric,
    new_balance numeric
)
language plpgsql
security definer
set search_path = public
as $$
declare
    v_total int;
    v_spent int;
    v_available int;
    v_new_balance numeric;
begin
    if p_referral_cost <= 0 then
        raise exception 'INVALID_REFERRAL_COST';
    end if;
    if p_usdt_amount <= 0 then
        raise exception 'INVALID_CONVERSION_AMOUNT';
    end if;

    perform pg_advisory_xact_lock(p_user_id);

    select greatest(
        0,
        (
            select count(*)::int
              from public.referrals
             where referrer_id = p_user_id
        )
        +
        coalesce((
            select sum(delta)
              from public.referral_adjustments
             where user_id = p_user_id
        ), 0)::int
    )
      into v_total;

    select
        coalesce((
            select sum(referral_cost)
              from public.referral_redemptions
             where user_id = p_user_id
        ), 0)::int
        +
        coalesce((
            select sum(refs_spent)
              from public.referral_conversions
             where user_id = p_user_id
        ), 0)::int
      into v_spent;

    v_available := v_total - v_spent;

    if v_available < p_referral_cost then
        raise exception 'INSUFFICIENT_REFERRALS';
    end if;

    insert into public.referral_conversions (
        user_id,
        refs_spent,
        amount
    )
    values (
        p_user_id,
        p_referral_cost,
        p_usdt_amount
    );

    update public.users
       set balance = coalesce(balance, 0) + p_usdt_amount
     where telegram_id = p_user_id
     returning balance into v_new_balance;

    if v_new_balance is null then
        raise exception 'USER_NOT_FOUND';
    end if;

    return query
    select
        v_total,
        v_spent + p_referral_cost,
        v_total - v_spent - p_referral_cost,
        p_usdt_amount,
        v_new_balance;
end;
$$;

grant execute on function public.spend_referral_balance(bigint, bigint, bigint, int)
    to service_role;

grant execute on function public.convert_referrals_to_wallet(bigint, int, numeric)
    to service_role;

-- ---------- PENDING REFERRAL COLUMNS ----------
alter table public.users
  add column if not exists pending_referral_by bigint default null,
  add column if not exists sub_verified boolean not null default false;

-- ---------- UNIQUE AMOUNT TAG ----------
alter table public.deposits
  add column if not exists unique_amount_tag text;

create unique index if not exists deposits_unique_amount_tag_idx
  on public.deposits (unique_amount_tag)
  where unique_amount_tag is not null;


-- ---------- RESELLER CUSTOM PRICING & HIDDEN PRODUCTS ----------
create table if not exists public.reseller_api_pricing (
    api_key_id bigint not null references public.reseller_api_keys(id) on delete cascade,
    product_id bigint not null references public.products(id) on delete cascade,
    sell_price numeric(14,2) not null check (sell_price >= 0),
    name_ar text,
    name_en text,
    desc_ar text,
    desc_en text,
    updated_at timestamptz not null default now(),
    primary key (api_key_id, product_id)
);

create table if not exists public.reseller_api_hidden (
    api_key_id bigint not null references public.reseller_api_keys(id) on delete cascade,
    product_id bigint not null references public.products(id) on delete cascade,
    primary key (api_key_id, product_id)
);

alter table public.reseller_api_pricing enable row level security;
alter table public.reseller_api_hidden enable row level security;

-- ---------- ADJUST BALANCE RPC ----------
create or replace function public.adjust_balance(
    p_telegram_id bigint,
    p_delta numeric
)
returns numeric
security definer
language plpgsql
as $$
declare
    v_new_balance numeric;
begin
    update public.users
    set balance = coalesce(balance, 0) + p_delta
    where telegram_id = p_telegram_id
    returning balance into v_new_balance;

    if v_new_balance is null then
        raise exception 'user_not_found:%', p_telegram_id;
    end if;

    return v_new_balance;
end;
$$;

grant execute on function public.adjust_balance(bigint, numeric) to anon, authenticated, service_role;






-- 0044 secure checkout and runtime (new installs)
-- Non-destructive upgrade. Historical balances and referral prices are never recalculated.
begin;
alter table public.users add column if not exists language_selected boolean not null default true;
alter table public.users alter column language_selected set default false;
alter table public.users add column if not exists referral_invalidated boolean not null default false;
alter table public.products add column if not exists catalog_hidden boolean not null default false;
alter table public.products add column if not exists api_auto_hidden boolean not null default false;

create table if not exists public.runtime_versions(name text primary key, version bigint not null default 0);
insert into public.runtime_versions(name) values('settings'),('catalog_static'),('catalog_availability') on conflict do nothing;
create or replace function public.bump_runtime_version() returns trigger language plpgsql security definer set search_path=public as $$
declare r jsonb;
begin
 if tg_table_name='settings' then
  r:=case when tg_op='DELETE' then to_jsonb(old) else to_jsonb(new) end;
  if r->>'key' like 'tenant:%' or r->>'key' like 'live_support%' or r->>'key' in ('api_shop_config','api_connection') then return null; end if;
  if tg_op='UPDATE' and old.value is not distinct from new.value then return null; end if;
  update runtime_versions set version=version+1 where name='settings';
 else
  update runtime_versions set version=version+1 where name='catalog_availability';
  if tg_op<>'UPDATE' or (to_jsonb(old)-array['stock','active','api_auto_hidden','catalog_hidden','sort_order','is_pinned','unlimited_stock','stashed_sort_order']) is distinct from (to_jsonb(new)-array['stock','active','api_auto_hidden','catalog_hidden','sort_order','is_pinned','unlimited_stock','stashed_sort_order']) then
   update runtime_versions set version=version+1 where name='catalog_static';
  end if;
 end if;
 return null;
end $$;
drop trigger if exists runtime_settings_revision on public.settings;
create trigger runtime_settings_revision after insert or update or delete on public.settings for each row execute function public.bump_runtime_version();
drop trigger if exists runtime_products_revision on public.products;
create trigger runtime_products_revision after insert or update or delete on public.products for each row execute function public.bump_runtime_version();
drop trigger if exists runtime_categories_revision on public.categories;
create trigger runtime_categories_revision after insert or update or delete on public.categories for each row execute function public.bump_runtime_version();
create or replace function public.runtime_revisions() returns jsonb language sql stable security definer set search_path=public as $$ select jsonb_object_agg(name,version) from runtime_versions $$;
create or replace function public.runtime_settings_snapshot() returns jsonb language sql stable security definer set search_path=public as $$
 select jsonb_build_object('revision',(select version from runtime_versions where name='settings'),'values',coalesce((select jsonb_object_agg(key,value) from settings where key not like 'tenant:%' and key not like 'live_support%' and key not in ('api_shop_config','api_connection')),'{}'::jsonb))
$$;
create or replace function public.catalog_availability() returns jsonb language sql stable security definer set search_path=public as $$
 select coalesce(jsonb_agg(jsonb_build_object('id',p.id,'stock',p.stock,'active',p.active and not p.catalog_hidden and not p.api_auto_hidden and coalesce(c.active,true),'catalog_hidden',p.catalog_hidden,'api_auto_hidden',p.api_auto_hidden,'unlimited_stock',p.unlimited_stock,'sort_order',p.sort_order,'is_pinned',p.is_pinned)),'[]'::jsonb) from products p left join categories c on c.id=p.category_id
$$;

create table if not exists public.wallet_operations(user_id bigint not null references public.users(telegram_id), operation_key text not null, delta numeric not null, balance_after numeric not null, primary key(user_id,operation_key));
create or replace function public.adjust_balance(p_telegram_id bigint,p_delta numeric) returns numeric language plpgsql security definer set search_path=public as $$
declare b numeric;
begin
 if p_delta is null or p_delta='NaN'::numeric then raise exception 'INVALID_AMOUNT'; end if;
 update users set balance=balance+p_delta where telegram_id=p_telegram_id and (p_delta>=0 or balance>=-p_delta) returning balance into b;
 if b is null then raise exception 'INSUFFICIENT_FUNDS_OR_USER_NOT_FOUND'; end if;
 return b;
end $$;
create or replace function public.apply_wallet_change(p_user_id bigint,p_delta numeric,p_type text,p_key text) returns numeric language plpgsql security definer set search_path=public as $$
declare b numeric; old_delta numeric;
begin
 perform 1 from users where telegram_id=p_user_id for update;
 select balance_after,delta into b,old_delta from wallet_operations where user_id=p_user_id and operation_key=p_key;
 if found then if old_delta<>p_delta then raise exception 'IDEMPOTENCY_CONFLICT'; end if; return b; end if;
 b:=adjust_balance(p_user_id,p_delta);
 insert into wallet_ledger(user_id,type,amount,reference) values(p_user_id,p_type,p_delta,p_key);
 insert into wallet_operations values(p_user_id,p_key,p_delta,b);
 return b;
end $$;

create table if not exists public.rate_limits(key text primary key,count integer not null,reset_at timestamptz not null);
create or replace function public.consume_rate_limit(p_key text,p_max integer,p_window_ms integer) returns jsonb language plpgsql security definer set search_path=public as $$
declare r rate_limits;
begin
 if p_max<1 or p_window_ms<1 then raise exception 'INVALID_RATE_LIMIT'; end if;
 insert into rate_limits values(p_key,1,now()+p_window_ms*interval '1 millisecond')
 on conflict(key) do update set count=case when rate_limits.reset_at<=now() then 1 else rate_limits.count+1 end,reset_at=case when rate_limits.reset_at<=now() then excluded.reset_at else rate_limits.reset_at end returning * into r;
 if random()<0.01 then delete from rate_limits where reset_at<now()-interval '1 day'; end if;
 return jsonb_build_object('ok',r.count<=p_max,'remaining',greatest(0,p_max-r.count),'retryAfterMs',greatest(0,extract(epoch from(r.reset_at-now()))*1000));
end $$;

create or replace function public.referral_setting(p_name text,p_default numeric) returns numeric language plpgsql stable security definer set search_path=public as $$
declare v jsonb; n numeric;
begin
 select value into v from settings where key in ('text.referral.'||p_name,'referral.'||p_name) order by (key like 'text.%') desc limit 1;
 begin n:=(v#>>'{}')::numeric; exception when others then n:=null; end;
 return coalesce(n,p_default);
end $$;
create or replace function public.confirm_referral_atomic(p_user_id bigint) returns jsonb language plpgsql security definer set search_path=public as $$
declare u users; rid bigint; lim integer;
begin
 -- Serialise confirmations for the same referrer as well as the referred user.
 select pending_referral_by into rid from users where telegram_id=p_user_id;
 if rid is not null then perform pg_advisory_xact_lock(rid); end if;
 select * into u from users where telegram_id=p_user_id for update;
 if u.sub_verified or u.referral_invalidated or u.pending_referral_by is null or rid=p_user_id then return jsonb_build_object('confirmed',false); end if;
 lim:=referral_setting('limit_per_day',0)::integer;
 update users set pending_referral_by=null,sub_verified=true where telegram_id=p_user_id;
 if not exists(select 1 from users where telegram_id=rid) or (lim>0 and (select count(*) from referrals where referrer_id=rid and created_at>=now()-interval '1 day')>=lim) then return jsonb_build_object('confirmed',false); end if;
 insert into referrals(referrer_id,referee_id) values(rid,p_user_id) on conflict do nothing;
 if not found then return jsonb_build_object('confirmed',false); end if;
 update users set referred_by=rid where telegram_id=p_user_id;
 return jsonb_build_object('confirmed',true,'referrer_id',rid);
end $$;
create or replace function public.convert_referrals_to_wallet(p_user_id bigint,p_referral_cost int default 20,p_usdt_amount numeric default 1.00)
returns table(total_referrals int,spent_referrals int,available_referrals int,converted_amount numeric,new_balance numeric)
language plpgsql security definer set search_path=public as $$
declare tot int; spent int; b numeric;
begin
 if p_referral_cost<1 or p_referral_cost<>referral_setting('cost',20) or p_usdt_amount<=0 or p_usdt_amount<>referral_setting('amount',1) then raise exception 'REFERRAL_RATE_CHANGED'; end if;
 perform pg_advisory_xact_lock(p_user_id);
 perform 1 from users where telegram_id=p_user_id for update;
 select greatest(0,(select count(*) from referrals where referrer_id=p_user_id)+coalesce((select sum(delta) from referral_adjustments where user_id=p_user_id),0))::int into tot;
 select (coalesce((select sum(referral_cost) from referral_redemptions where user_id=p_user_id),0)+coalesce((select sum(refs_spent) from referral_conversions where user_id=p_user_id),0))::int into spent;
 if tot-spent<p_referral_cost then raise exception 'INSUFFICIENT_REFERRALS'; end if;
 insert into referral_conversions(user_id,refs_spent,amount) values(p_user_id,p_referral_cost,p_usdt_amount);
 update users set balance=balance+p_usdt_amount,referral_earned_total=referral_earned_total+p_usdt_amount,referral_transferred=referral_transferred+p_usdt_amount where telegram_id=p_user_id returning balance into b;
 if b is null then raise exception 'USER_NOT_FOUND'; end if;
 insert into wallet_ledger(user_id,type,amount,reference) values(p_user_id,'referral_convert',p_usdt_amount,'referral_convert:'||p_referral_cost);
 return query select tot,spent+p_referral_cost,tot-spent-p_referral_cost,p_usdt_amount,b;
end $$;

-- Additional transactional checkout functions are defined below before COMMIT.
alter table public.orders add column if not exists checkout_key text;
alter table public.orders add column if not exists allocated_items text[] not null default '{}';
alter table public.orders add column if not exists stock_reserved boolean not null default false;
alter table public.orders add column if not exists is_preorder boolean not null default false;
alter table public.orders add column if not exists fulfillment_state text not null default 'legacy';
alter table public.orders add column if not exists delivery_lease_until timestamptz;
create unique index if not exists orders_checkout_key_unique on public.orders(user_id,checkout_key) where checkout_key is not null;

create or replace function public.spend_referral_balance(p_user_id bigint,p_product_id bigint,p_order_id bigint,p_referral_cost int)
returns table(total_referrals int,spent_referrals int,available_referrals int) language plpgsql security definer set search_path=public as $$
declare tot int; spent int; expected int; existing int;
begin
 perform pg_advisory_xact_lock(p_user_id);
 select p.referral_required_count*o.qty into expected from products p join orders o on o.product_id=p.id where o.id=p_order_id and o.user_id=p_user_id and p.id=p_product_id;
 if expected is null or expected<=0 or p_referral_cost<>expected then raise exception 'INVALID_REFERRAL_COST'; end if;
 select greatest(0,(select count(*) from referrals where referrer_id=p_user_id)+coalesce((select sum(delta) from referral_adjustments where user_id=p_user_id),0))::int into tot;
 select (coalesce((select sum(referral_cost) from referral_redemptions where user_id=p_user_id),0)+coalesce((select sum(refs_spent) from referral_conversions where user_id=p_user_id),0))::int into spent;
 select referral_cost into existing from referral_redemptions where order_id=p_order_id;
 if existing is not null then return query select tot,spent,greatest(0,tot-spent);return;end if;
 if tot-spent<p_referral_cost then raise exception 'INSUFFICIENT_REFERRALS'; end if;
 insert into referral_redemptions(user_id,product_id,order_id,referral_cost) values(p_user_id,p_product_id,p_order_id,p_referral_cost);
 return query select tot,spent+p_referral_cost,tot-spent-p_referral_cost;
end $$;

create or replace function public.create_checkout_order(p_user_id bigint,p_product_id bigint,p_qty integer,p_unit_price numeric,p_total numeric,p_discount numeric,p_promo_id bigint,p_key text,p_kind text default 'wallet',p_allow_preorder boolean default false,p_api_key_id bigint default null)
returns jsonb language plpgsql security definer set search_path=public as $$
declare u users; p products; o orders; items text[]; b numeric; pre boolean; pool integer;
begin
 if p_kind not in ('wallet','referral','direct','api') or p_key is null or length(p_key)>200 or length(p_key)<1 or p_qty<1 or p_qty>999 or p_total<0 or p_unit_price<0 or p_discount<0 or p_total<>round(p_unit_price*p_qty-p_discount,2) then raise exception 'INVALID_CHECKOUT';end if;
 -- Same lock order as referral conversion, deposit approval and wallet changes.
 perform pg_advisory_xact_lock(p_user_id);
 select * into u from users where telegram_id=p_user_id for update;
 if not found or u.is_banned then raise exception 'USER_UNAVAILABLE';end if;
 select * into o from orders where user_id=p_user_id and checkout_key=p_key;
 if found then if o.product_id<>p_product_id or o.qty<>p_qty or o.total<>p_total then raise exception 'IDEMPOTENCY_CONFLICT';end if;return jsonb_build_object('order',to_jsonb(o),'balance',u.balance,'replayed',true);end if;
 select * into p from products where id=p_product_id for update;
 if not found or not p.active or p.catalog_hidden or p.api_auto_hidden or exists(select 1 from categories where id=p.category_id and not active) then raise exception 'PRODUCT_UNAVAILABLE';end if;
 if p_api_key_id is not null and (not exists(select 1 from reseller_api_keys where id=p_api_key_id and user_id=p_user_id and active) or exists(select 1 from reseller_api_hidden where api_key_id=p_api_key_id and product_id=p_product_id)) then raise exception 'API_KEY_OR_PRODUCT_UNAVAILABLE';end if;
 pre:=not p.unlimited_stock and p.stock<p_qty;
 if pre and not p_allow_preorder then raise exception 'OUT_OF_STOCK';end if;
 insert into orders(user_id,product_id,product_name,qty,unit_price,total,discount,promo_id,checkout_key,is_preorder,fulfillment_state,delivery)
 values(p_user_id,p_product_id,p.name,p_qty,p_unit_price,p_total,p_discount,p_promo_id,p_key,pre,'pending',case when pre then 'Preorder pending' else 'Delivery pending' end) returning * into o;
 b:=u.balance;
 if p_kind in ('wallet','api') then b:=apply_wallet_change(p_user_id,-p_total,'wallet_purchase','order:'||o.id);
 elsif p_kind='referral' then perform * from spend_referral_balance(p_user_id,p_product_id,o.id,p.referral_required_count*p_qty);end if;
 if not pre then
  if not p.unlimited_stock then update products set stock=stock-p_qty where id=p.id;end if;
  select count(*) into pool from product_items where product_id=p.id;
  if pool>0 and not exists(select 1 from supplier_product_links where local_product_id=p.id and auto_order) and coalesce(p.note,'') not like '%[API_PRODUCT_ID:%' then
   with picked as(select id from product_items where product_id=p.id and consumed_at is null order by id for update skip locked limit p_qty), claimed as(update product_items i set consumed_at=now(),consumed_order_id=o.id from picked where i.id=picked.id returning i.payload) select array_agg(payload) into items from claimed;
   if coalesce(cardinality(items),0)<>p_qty then raise exception 'DELIVERY_NOT_READY';end if;
  end if;
  update orders set stock_reserved=true,allocated_items=coalesce(items,'{}') where id=o.id returning * into o;
 end if;
 if p_api_key_id is not null then insert into reseller_api_orders(user_id,api_key_id,order_id,product_id,qty,total,request_id) values(p_user_id,p_api_key_id,o.id,p_product_id,p_qty,p_total,p_key);end if;
 return jsonb_build_object('order',to_jsonb(o),'balance',b,'replayed',false);
end $$;

create or replace function public.claim_product_items(p_product_id bigint,p_qty integer,p_order_id bigint) returns text[] language plpgsql security definer set search_path=public as $$
declare o orders; items text[]; n integer;
begin
 select * into o from orders where id=p_order_id for update;
 if not found or o.product_id<>p_product_id or o.qty<>p_qty or o.status<>'paid' then raise exception 'INVALID_ORDER';end if;
 if cardinality(o.allocated_items)>0 then return o.allocated_items;end if;
 select count(*) into n from product_items where product_id=p_product_id;
 if n=0 then return '{}';end if;
 with picked as(select id from product_items where product_id=p_product_id and consumed_at is null order by id for update skip locked limit p_qty), claimed as(update product_items i set consumed_at=now(),consumed_order_id=p_order_id from picked where i.id=picked.id returning i.payload) select array_agg(payload) into items from claimed;
 if coalesce(cardinality(items),0)<>p_qty then raise exception 'DELIVERY_NOT_READY';end if;
 update orders set allocated_items=items where id=p_order_id;
 return items;
end $$;
create or replace function public.change_product_stock(p_product_id bigint,p_delta integer) returns integer language plpgsql security definer set search_path=public as $$
declare n int;
begin
 update products set stock=stock+p_delta where id=p_product_id and not unlimited_stock and stock+p_delta>=0 returning stock into n;
 if n is null and not exists(select 1 from products where id=p_product_id and unlimited_stock) then raise exception 'OUT_OF_STOCK';end if;
 return n;
end $$;
create or replace function public.product_pool_counts() returns table(product_id bigint,available bigint) language sql stable security definer set search_path=public as $$ select product_id,count(*) from product_items where consumed_at is null group by product_id $$;

create table if not exists public.payment_quotes(id bigserial primary key,user_id bigint not null references public.users(telegram_id),provider text not null,address text not null,base_amount numeric not null check(base_amount>0),exact_amount numeric not null,asset text not null default 'USDT',conversion_rate numeric not null default 1,expires_at timestamptz not null,consumed_by bigint unique);
alter table public.deposits add column if not exists quote_id bigint references public.payment_quotes(id);
alter table public.deposits add column if not exists fulfilled_order_id bigint references public.orders(id);
alter table public.deposits add column if not exists approval_result jsonb;
create or replace function public.reserve_payment_quote(p_user_id bigint,p_provider text,p_address text,p_base numeric,p_asset text default 'USDT',p_rate numeric default 1) returns jsonb language plpgsql security definer set search_path=public as $$
declare q payment_quotes; step numeric; candidate numeric; i int;
begin
 if p_base<=0 or p_base>1000000 or p_rate<=0 or p_asset not in ('USDT','LTC') or length(p_address)<3 then raise exception 'INVALID_QUOTE';end if;
 perform pg_advisory_xact_lock(hashtextextended(p_provider||':'||p_address,0));
 for i in 1..999 loop
  step:=case when p_asset='LTC' then 0.00000001 else 0.0001 end;
  candidate:=case when p_asset='LTC' then round(p_base/p_rate,8) else round(p_base,2) end+i*step;
  if not exists(select 1 from payment_quotes where provider=p_provider and address=p_address and exact_amount=candidate and expires_at>now()) then
   insert into payment_quotes(user_id,provider,address,base_amount,exact_amount,asset,conversion_rate,expires_at) values(p_user_id,p_provider,p_address,p_base,candidate,p_asset,p_rate,now()+case when p_asset='LTC' then interval '10 minutes' else interval '30 minutes' end) returning * into q;
   return to_jsonb(q);
  end if;
 end loop;
 raise exception 'QUOTE_CAPACITY_REACHED';
end $$;
create or replace function public.approve_deposit_atomic(p_id bigint,p_amount numeric,p_tx_hash text default null) returns jsonb language plpgsql security definer set search_path=public as $$
declare d deposits; q payment_quotes; checkout jsonb; result jsonb; b numeric; i jsonb; paid numeric; orderid bigint;
begin
 select * into d from deposits where id=p_id;
 if not found then raise exception 'DEPOSIT_NOT_FOUND';end if;
 perform pg_advisory_xact_lock(d.user_id);
 perform 1 from users where telegram_id=d.user_id for update;
 select * into d from deposits where id=p_id for update;
 if d.status='approved' and d.approval_result is not null then return d.approval_result||jsonb_build_object('replayed',true);end if;
 if d.status<>'pending' or p_amount<=0 or p_amount='NaN'::numeric then raise exception 'DEPOSIT_NOT_PENDING_OR_AMOUNT_INVALID';end if;
 if p_tx_hash is not null and exists(select 1 from deposits where id<>d.id and tx_hash=p_tx_hash) then raise exception 'TRANSACTION_ALREADY_USED';end if;
 paid:=p_amount;
 if d.quote_id is not null then
  select * into q from payment_quotes where id=d.quote_id for update;
  if q.user_id<>d.user_id or q.consumed_by is not null and q.consumed_by<>d.id then raise exception 'QUOTE_OWNERSHIP';end if;
  if q.asset='USDT' and abs(p_amount-q.exact_amount)>0.0000001 then raise exception 'PAYMENT_AMOUNT_MISMATCH';end if;
  if q.asset='LTC' and abs(p_amount-q.base_amount)>0.01 then raise exception 'PAYMENT_AMOUNT_MISMATCH';end if;
  update payment_quotes set consumed_by=d.id where id=q.id;
 end if;
 if d.order_intent is not null then
  i:=d.order_intent;
  if paid<(i->>'total')::numeric then raise exception 'INSUFFICIENT_PAYMENT';end if;
  if exists(select 1 from products where id=(i->>'product_id')::bigint and active and not catalog_hidden and not api_auto_hidden and (category_id is null or exists(select 1 from categories where id=products.category_id and active))) then
   checkout:=create_checkout_order(d.user_id,(i->>'product_id')::bigint,(i->>'qty')::int,(i->>'unit_price')::numeric,(i->>'total')::numeric,coalesce((i->>'discount')::numeric,0),(i->>'promo_id')::bigint,'deposit:'||d.id,'direct',true);
   orderid:=(checkout->'order'->>'id')::bigint;
   paid:=greatest(0,paid-(i->>'total')::numeric);
  end if;
 end if;
 if round(paid,2)>0 then b:=apply_wallet_change(d.user_id,round(paid,2),'deposit_credit','deposit:'||d.id);else select balance into b from users where telegram_id=d.user_id;end if;
 result:=jsonb_build_object('balance',b,'order',checkout->'order','refunded',d.order_intent is not null and orderid is null,'replayed',false);
 update deposits set status='approved',amount=round(p_amount,2),tx_hash=coalesce(p_tx_hash,tx_hash),fulfilled_order_id=orderid,approval_result=result,updated_at=now() where id=d.id;
 return result;
end $$;
create or replace function public.redeem_gift_atomic(p_user_id bigint,p_code text) returns jsonb language plpgsql security definer set search_path=public as $$
declare g gift_codes; b numeric;
begin
 select * into g from gift_codes where code=p_code for update;
 if not found or g.amount<=0 or g.expires_at<=now() then raise exception 'GIFT_UNAVAILABLE';end if;
 if (g.max_redemptions is not null and (select count(*) from gift_code_redemptions where code=p_code)>=g.max_redemptions) or (select count(*) from gift_code_redemptions where code=p_code and user_id=p_user_id)>=g.per_user_limit then raise exception 'GIFT_LIMIT';end if;
 b:=adjust_balance(p_user_id,g.amount);
 insert into gift_code_redemptions(code,user_id,amount) values(p_code,p_user_id,g.amount);
 insert into wallet_ledger(user_id,type,amount,reference) values(p_user_id,'gift_credit',g.amount,p_code);
 return jsonb_build_object('amount',g.amount,'balance',b);
end $$;
create table if not exists public.supplier_order_attempts(order_id bigint primary key references public.orders(id),state text not null default 'pending',result jsonb,created_at timestamptz not null default now());
create table if not exists public.delivery_form_sessions(user_id bigint primary key, data jsonb not null,updated_at timestamptz not null default now());

create or replace function public.begin_order_delivery(p_order_id bigint) returns boolean language plpgsql security definer set search_path=public as $$
begin
 update orders set fulfillment_state='processing',delivery_lease_until=now()+interval '5 minutes' where id=p_order_id and status='paid' and (fulfillment_state='pending' or fulfillment_state='processing' and delivery_lease_until<now());
 return found;
end $$;
create or replace function public.pending_checkout_deliveries() returns setof public.orders language sql stable security definer set search_path=public as $$ select * from orders where checkout_key is not null and status='paid' and (fulfillment_state='pending' or fulfillment_state='processing' and delivery_lease_until<now()) order by id limit 25 $$;

create or replace function public.tenant_registry_mutation(p_action text,p_id text,p_payload jsonb) returns jsonb language plpgsql security definer set search_path=public as $$
declare tenant jsonb; ids jsonb; stamp timestamptz;
begin
 perform pg_advisory_xact_lock(hashtextextended('tenant:registry',0));
 select value into tenant from settings where key='tenant:'||p_id;
 select coalesce(value,'[]') into ids from settings where key='tenant:list';ids:=coalesce(ids,'[]');
 if p_action='add' then
  if tenant is not null then raise exception 'TENANT_EXISTS';end if;
  if p_payload->>'id'<>p_id then raise exception 'INVALID_TENANT';end if;
  tenant:=p_payload;
  if not ids ? p_id then ids:=ids||to_jsonb(p_id);end if;
 elsif p_action='delete' then
  delete from settings where key='tenant:'||p_id;
  select coalesce(jsonb_agg(v),'[]') into ids from jsonb_array_elements(ids) v where v<>to_jsonb(p_id);
 elsif tenant is null then raise exception 'TENANT_NOT_FOUND';
 elsif p_action='status' then
  if p_payload->>'status' not in ('active','suspended','expired') then raise exception 'INVALID_TENANT_STATUS';end if;
  tenant:=tenant||jsonb_build_object('status',p_payload->>'status','updated_at',now());
 elsif p_action='extend' then
  if (p_payload->>'days')::int<1 then raise exception 'INVALID_DAYS';end if;
  stamp:=greatest((tenant->>'subscription_end')::timestamptz,now())+(p_payload->>'days')::int*interval '1 day';
  tenant:=tenant||jsonb_build_object('subscription_end',stamp,'status','active','updated_at',now());
 else raise exception 'INVALID_TENANT_ACTION';end if;
 if p_action<>'delete' then insert into settings(key,value) values('tenant:'||p_id,tenant) on conflict(key) do update set value=excluded.value;end if;
 insert into settings(key,value) values('tenant:list',ids) on conflict(key) do update set value=excluded.value;
 return tenant;
end $$;

create or replace function public.shop_statistics(p_user_id bigint default null,p_days integer default null) returns jsonb language sql stable security definer set search_path=public as $$
 with paid as(select * from orders where status='paid' and (p_user_id is null or user_id=p_user_id) and (p_days is null or created_at>=now()-greatest(1,least(365,p_days))*interval '1 day')),
 credited as(select * from deposits where status='approved' and (p_user_id is null or user_id=p_user_id) and (p_days is null or created_at>=now()-greatest(1,least(365,p_days))*interval '1 day'))
 select jsonb_build_object('users',(select count(*) from users),'orders',(select count(*) from paid),'revenue',coalesce((select sum(total) from paid),0),'spent',coalesce((select sum(total) from paid),0),'items',coalesce((select sum(qty) from paid),0),'units',coalesce((select sum(qty) from paid),0),'lastOrderAt',(select max(created_at) from paid),'deposits',coalesce((select sum(amount) from credited),0),'pending_deposits',(select count(*) from deposits where status='pending'),'active_products',(select count(*) from products where active and not catalog_hidden and not api_auto_hidden),'active_categories',(select count(*) from categories where active),'days',p_days,'unique_buyers',(select count(distinct user_id) from paid),'approved_deposits',(select count(*) from credited),'deposit_amount',coalesce((select sum(amount) from credited),0),'new_users',(select count(*) from users where p_days is null or joined_at>=now()-greatest(1,least(365,p_days))*interval '1 day'))
$$;
create or replace function public.product_sales_statistics(p_days integer default null,p_limit integer default 50) returns jsonb language sql stable security definer set search_path=public as $$
 select coalesce(jsonb_agg(to_jsonb(r)),'[]') from (select o.product_id,coalesce(p.name,max(o.product_name)) as product_name,count(*) as orders,sum(o.qty) as units_sold,sum(o.total) as revenue,max(o.created_at) as last_sold_at,p.stock as stock_left from orders o left join products p on p.id=o.product_id where o.status='paid' and (p_days is null or o.created_at>=now()-greatest(1,least(365,p_days))*interval '1 day') group by o.product_id,case when o.product_id is null then o.product_name else null end,p.name,p.stock order by revenue desc limit greatest(1,least(500,p_limit))) r
$$;
create or replace function public.reserve_preorder_stock(p_order_id bigint) returns boolean language plpgsql security definer set search_path=public as $$
declare o orders; p products;
begin
 select * into o from orders where id=p_order_id for update;
 if not found or o.status<>'paid' then return false;end if;
 if o.stock_reserved then return true;end if;
 select * into p from products where id=o.product_id for update;
 if not found or not p.active or p.catalog_hidden or p.api_auto_hidden or not p.unlimited_stock and p.stock<o.qty then return false;end if;
 if not p.unlimited_stock then update products set stock=stock-o.qty where id=p.id;end if;
 update orders set stock_reserved=true where id=o.id;return true;
end $$;
create or replace function public.release_preorder_stock(p_order_id bigint) returns void language plpgsql security definer set search_path=public as $$
declare o orders;
begin
 select * into o from orders where id=p_order_id for update;
 if o.stock_reserved and o.fulfillment_state<>'delivered' then update products set stock=stock+o.qty where id=o.product_id and not unlimited_stock;update orders set stock_reserved=false where id=o.id;end if;
end $$;

create or replace function public.api_account_statistics(p_user_id bigint) returns jsonb language sql stable security definer set search_path=public as $$
 select jsonb_build_object('total_orders',count(*),'completed_orders',count(*) filter(where o.fulfillment_state='delivered' or o.fulfillment_state='legacy'),'pending_orders',count(*) filter(where o.fulfillment_state not in ('delivered','legacy')),'total_revenue',coalesce(sum(a.total) filter(where o.status='paid'),0)) from reseller_api_orders a join orders o on o.id=a.order_id where a.user_id=p_user_id
$$;

-- No browser/client role can invoke privileged financial RPCs or inspect secret-bearing tables.
do $$ declare t text; f record; begin
 foreach t in array array['wallet_ledger','gift_codes','gift_code_redemptions','user_price_overrides','promos','reseller_api_keys','reseller_api_orders','supplier_api_sources','supplier_product_links','supplier_order_logs','wallet_operations','payment_quotes','rate_limits','delivery_form_sessions','supplier_order_attempts','runtime_versions'] loop
  execute format('alter table public.%I enable row level security',t);
  execute format('revoke all on public.%I from anon,authenticated',t);
 end loop;
 revoke all on public.products_view from anon,authenticated;
 for f in select p.oid::regprocedure as signature from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.prosecdef and p.proname in ('bump_runtime_version','runtime_revisions','runtime_settings_snapshot','catalog_availability','adjust_balance','apply_wallet_change','consume_rate_limit','referral_setting','confirm_referral_atomic','convert_referrals_to_wallet','spend_referral_balance','create_checkout_order','claim_product_items','change_product_stock','product_pool_counts','reserve_payment_quote','approve_deposit_atomic','redeem_gift_atomic','begin_order_delivery','pending_checkout_deliveries','tenant_registry_mutation','shop_statistics','product_sales_statistics','reserve_preorder_stock','release_preorder_stock','api_account_statistics') loop
  execute format('revoke all on function %s from public,anon,authenticated',f.signature);
  execute format('grant execute on function %s to service_role',f.signature);
 end loop;
end $$;
commit;

-- WWC PLATFORM 0045
begin;
alter table products add column if not exists wwc_source_id text;
create index if not exists wwc_source_product on products(wwc_source_id) where wwc_source_id is not null;
alter table users drop constraint if exists users_language_check;
alter table users add constraint users_language_check check(language in ('en','ar','vi','zh'));
create table if not exists wwc_shop_sessions(key text primary key,value jsonb not null,expires_at timestamptz not null);
create index if not exists wwc_shop_sessions_expiry on wwc_shop_sessions(expires_at);
create or replace function wwc_shop_sessions_cleanup() returns integer language plpgsql security definer set search_path=public as $$ declare n integer;begin
 delete from wwc_shop_sessions where key in(select key from wwc_shop_sessions where expires_at<now() order by expires_at limit 1000);get diagnostics n=row_count;return n;
end $$;
create table if not exists wwc_platform_shops(
 id text primary key,owner_id bigint not null,owner_lang text not null default 'en',bot_id bigint not null unique,bot_username text not null,
 token_fingerprint text not null unique,database_fingerprint text not null unique,secret_box text not null,
 status text not null default 'setup' check(status in('setup','active','idle','locked','suspended')),
 plan text not null default 'free' check(plan in('free','paid')),paid_until timestamptz,
 idle_strikes integer not null default 0 check(idle_strikes between 0 and 3),last_customer_at timestamptz,last_purchase_at timestamptz,
 last_active_at timestamptz not null default now(),warned_at timestamptz,activation_requested_at timestamptz,created_at timestamptz not null default now(),updated_at timestamptz not null default now()
);
alter table wwc_platform_shops add column if not exists activation_requested_at timestamptz;
create index if not exists wwc_platform_owner on wwc_platform_shops(owner_id,created_at desc);
create index if not exists wwc_platform_active on wwc_platform_shops(last_active_at) where status='active';
create table if not exists wwc_platform_receipts(receipt text primary key,shop_id text not null references wwc_platform_shops(id),actor bigint not null,days integer not null,created_at timestamptz not null default now());
create table if not exists wwc_platform_events(id text primary key,shop_id text not null references wwc_platform_shops(id),kind text not null,created_at timestamptz not null default now());
create table if not exists wwc_platform_nonces(nonce text primary key,created_at timestamptz not null default now());
create table if not exists wwc_platform_lease(id integer primary key check(id=1),owner text not null,expires_at timestamptz not null);
create or replace function wwc_platform_nonce(p_nonce text) returns boolean language plpgsql security definer set search_path=public as $$ begin
 delete from wwc_platform_nonces where created_at<now()-interval '10 minutes';insert into wwc_platform_nonces(nonce) values(p_nonce) on conflict do nothing;return found;
end $$;
create or replace function wwc_platform_lease_claim(p_owner text,p_release boolean default false) returns boolean language plpgsql security definer set search_path=public as $$ begin
 if p_release then delete from wwc_platform_lease where id=1 and owner=p_owner;return true;end if;
 insert into wwc_platform_lease values(1,p_owner,now()+interval '90 seconds') on conflict(id) do update set owner=excluded.owner,expires_at=excluded.expires_at where wwc_platform_lease.owner=p_owner or wwc_platform_lease.expires_at<now();return found;
end $$;
create or replace function wwc_platform_mutate(p_action text,p_id text,p_actor bigint,p_payload jsonb default '{}'::jsonb) returns jsonb language plpgsql security definer set search_path=public as $$
declare s wwc_platform_shops;su boolean:=coalesce((p_payload->>'trusted_super')::boolean,false);r wwc_platform_receipts;d integer;
begin
 perform pg_advisory_xact_lock(88210045);
 if p_action='register' then
  if(select count(*) from wwc_platform_shops where owner_id=p_actor)>=(p_payload->>'max_owner_shops')::integer then raise exception 'OWNER_SHOP_LIMIT';end if;
  insert into wwc_platform_shops(id,owner_id,owner_lang,bot_id,bot_username,token_fingerprint,database_fingerprint,secret_box) values(p_id,p_actor,coalesce(p_payload->>'owner_lang','en'),(p_payload->>'bot_id')::bigint,p_payload->>'bot_username',p_payload->>'token_fingerprint',p_payload->>'database_fingerprint',p_payload->>'secret_box') returning * into s;return to_jsonb(s);
 end if;
 select * into s from wwc_platform_shops where id=p_id for update;if not found then raise exception 'SHOP_NOT_FOUND';end if;
 if s.owner_id<>p_actor and not su then raise exception 'FORBIDDEN';end if;
 if p_action in('activate','resume','super_resume') then
  if s.status='suspended' and not su then raise exception 'ADMIN_SUSPENDED';end if;
  if s.idle_strikes>=3 and not su then raise exception 'OWNER_UNLOCK_REQUIRED';end if;
  if s.status<>'active' and(select count(*) from wwc_platform_shops where status='active')>=(p_payload->>'max_active_shops')::integer then raise exception 'HOST_CAPACITY';end if;
  update wwc_platform_shops set status='active',last_active_at=now(),warned_at=null,activation_requested_at=null,idle_strikes=case when su then 0 else idle_strikes end where id=p_id;
 elsif p_action='queue' then
  if s.status in('locked','suspended') then raise exception 'OWNER_UNLOCK_REQUIRED';end if;
  update wwc_platform_shops set activation_requested_at=coalesce(activation_requested_at,now()) where id=p_id;
 elsif p_action='unqueue' then update wwc_platform_shops set activation_requested_at=null where id=p_id;
 elsif p_action='pause' then update wwc_platform_shops set status=case when su then 'suspended' else 'idle' end,activation_requested_at=null where id=p_id;
 elsif p_action='subscribe' then
  d:=(p_payload->>'days')::integer;if d<1 or d>365 then raise exception 'INVALID_DAYS';end if;
  select * into r from wwc_platform_receipts where receipt=p_payload->>'receipt';if found then if r.shop_id<>p_id or r.actor<>p_actor then raise exception 'RECEIPT_CONFLICT';end if;return to_jsonb(s);end if;
  insert into wwc_platform_receipts(receipt,shop_id,actor,days) values(p_payload->>'receipt',p_id,p_actor,d);
  update wwc_platform_shops set plan='paid',paid_until=greatest(coalesce(paid_until,now()),now())+make_interval(days=>d) where id=p_id;
 elsif p_action='activity' then
  if not su then raise exception 'FORBIDDEN';end if;
  insert into wwc_platform_events(id,shop_id,kind) values(p_payload->>'event',p_id,p_payload->>'kind') on conflict do nothing;
  if found then update wwc_platform_shops set last_active_at=now(),warned_at=null,last_customer_at=case when p_payload->>'kind'='customer' then now() else last_customer_at end,last_purchase_at=case when p_payload->>'kind'='purchase' then now() else last_purchase_at end,idle_strikes=case when p_payload->>'kind'='purchase' then 0 else idle_strikes end where id=p_id;end if;
 elsif p_action='idle' then
  if not su then raise exception 'FORBIDDEN';end if;
  if s.status='active' and s.last_active_at<now()-make_interval(hours=>(p_payload->>'hours')::integer) then update wwc_platform_shops set idle_strikes=least(3,idle_strikes+1),status=case when idle_strikes+1>=3 then 'locked' else 'idle' end where id=p_id;end if;
 elsif p_action='warn' then
  if not su then raise exception 'FORBIDDEN';end if;update wwc_platform_shops set warned_at=now() where id=p_id and warned_at is null;
 elsif p_action='credentials' then
  update wwc_platform_shops set secret_box=p_payload->>'secret_box',token_fingerprint=p_payload->>'token_fingerprint',database_fingerprint=p_payload->>'database_fingerprint',status=case when status in('locked','suspended') then status else 'setup' end where id=p_id;
 else raise exception 'INVALID_ACTION';end if;
 update wwc_platform_shops set updated_at=now() where id=p_id returning * into s;
 delete from wwc_platform_events where kind='customer' and created_at<now()-interval '7 days';return to_jsonb(s);
end $$;
create or replace function shop_platform_statistics() returns jsonb language sql stable security definer set search_path=public as $$
 select jsonb_build_object('users',(select count(*) from users),'orders',(select count(*) from orders where status='paid'),'revenue',(select coalesce(sum(total),0) from orders where status='paid'),
 'known_supplier_profit',(select coalesce(sum(o.total-coalesce((a.result->>'total_price')::numeric,(a.result->'data'->>'total_price')::numeric)),0) from orders o join supplier_order_attempts a on a.order_id=o.id where o.status='paid' and a.state='success' and(a.result ? 'total_price' or a.result->'data' ? 'total_price')),
 'last_purchase_at',(select max(created_at) from orders where status='paid' and fulfillment_state='delivered'))
$$;
do $$ declare n text;f record;begin
 foreach n in array array['wwc_shop_sessions','wwc_platform_shops','wwc_platform_receipts','wwc_platform_events','wwc_platform_nonces','wwc_platform_lease'] loop execute format('alter table public.%I enable row level security',n);execute format('revoke all on public.%I from public,anon,authenticated',n);execute format('grant all on public.%I to service_role',n);end loop;
 for f in select p.oid::regprocedure sig from pg_proc p join pg_namespace ns on ns.oid=p.pronamespace where ns.nspname='public' and(p.proname like 'wwc_platform_%' or p.proname in('shop_platform_statistics','wwc_shop_sessions_cleanup')) loop execute format('revoke all on function %s from public,anon,authenticated',f.sig);execute format('grant execute on function %s to service_role',f.sig);end loop;
end $$;
commit;

-- WWC PLATFORM 0046
-- Preserve the current bot username and owner language when rotating credentials.
begin;
create or replace function wwc_platform_mutate(p_action text,p_id text,p_actor bigint,p_payload jsonb default '{}'::jsonb) returns jsonb language plpgsql security definer set search_path=public as $$
declare s wwc_platform_shops;su boolean:=coalesce((p_payload->>'trusted_super')::boolean,false);r wwc_platform_receipts;d integer;
begin
 perform pg_advisory_xact_lock(88210045);
 if p_action='register' then
  if(select count(*) from wwc_platform_shops where owner_id=p_actor)>=(p_payload->>'max_owner_shops')::integer then raise exception 'OWNER_SHOP_LIMIT';end if;
  insert into wwc_platform_shops(id,owner_id,owner_lang,bot_id,bot_username,token_fingerprint,database_fingerprint,secret_box) values(p_id,p_actor,coalesce(p_payload->>'owner_lang','en'),(p_payload->>'bot_id')::bigint,p_payload->>'bot_username',p_payload->>'token_fingerprint',p_payload->>'database_fingerprint',p_payload->>'secret_box') returning * into s;return to_jsonb(s);
 end if;
 select * into s from wwc_platform_shops where id=p_id for update;if not found then raise exception 'SHOP_NOT_FOUND';end if;
 if s.owner_id<>p_actor and not su then raise exception 'FORBIDDEN';end if;
 if p_action in('activate','resume','super_resume') then
  if s.status='suspended' and not su then raise exception 'ADMIN_SUSPENDED';end if;
  if s.idle_strikes>=3 and not su then raise exception 'OWNER_UNLOCK_REQUIRED';end if;
  if s.status<>'active' and(select count(*) from wwc_platform_shops where status='active')>=(p_payload->>'max_active_shops')::integer then raise exception 'HOST_CAPACITY';end if;
  update wwc_platform_shops set status='active',last_active_at=now(),warned_at=null,activation_requested_at=null,idle_strikes=case when su then 0 else idle_strikes end where id=p_id;
 elsif p_action='queue' then
  if s.status in('locked','suspended') then raise exception 'OWNER_UNLOCK_REQUIRED';end if;
  update wwc_platform_shops set activation_requested_at=coalesce(activation_requested_at,now()) where id=p_id;
 elsif p_action='unqueue' then update wwc_platform_shops set activation_requested_at=null where id=p_id;
 elsif p_action='pause' then update wwc_platform_shops set status=case when su then 'suspended' else 'idle' end,activation_requested_at=null where id=p_id;
 elsif p_action='subscribe' then
  d:=(p_payload->>'days')::integer;if d<1 or d>365 then raise exception 'INVALID_DAYS';end if;
  select * into r from wwc_platform_receipts where receipt=p_payload->>'receipt';if found then if r.shop_id<>p_id or r.actor<>p_actor then raise exception 'RECEIPT_CONFLICT';end if;return to_jsonb(s);end if;
  insert into wwc_platform_receipts(receipt,shop_id,actor,days) values(p_payload->>'receipt',p_id,p_actor,d);
  update wwc_platform_shops set plan='paid',paid_until=greatest(coalesce(paid_until,now()),now())+make_interval(days=>d) where id=p_id;
 elsif p_action='activity' then
  if not su then raise exception 'FORBIDDEN';end if;
  insert into wwc_platform_events(id,shop_id,kind) values(p_payload->>'event',p_id,p_payload->>'kind') on conflict do nothing;
  if found then update wwc_platform_shops set last_active_at=now(),warned_at=null,last_customer_at=case when p_payload->>'kind'='customer' then now() else last_customer_at end,last_purchase_at=case when p_payload->>'kind'='purchase' then now() else last_purchase_at end,idle_strikes=case when p_payload->>'kind'='purchase' then 0 else idle_strikes end where id=p_id;end if;
 elsif p_action='idle' then
  if not su then raise exception 'FORBIDDEN';end if;
  if s.status='active' and s.last_active_at<now()-make_interval(hours=>(p_payload->>'hours')::integer) then update wwc_platform_shops set idle_strikes=least(3,idle_strikes+1),status=case when idle_strikes+1>=3 then 'locked' else 'idle' end where id=p_id;end if;
 elsif p_action='warn' then
  if not su then raise exception 'FORBIDDEN';end if;update wwc_platform_shops set warned_at=now() where id=p_id and warned_at is null;
 elsif p_action='credentials' then
  update wwc_platform_shops set secret_box=p_payload->>'secret_box',token_fingerprint=p_payload->>'token_fingerprint',database_fingerprint=p_payload->>'database_fingerprint',bot_username=coalesce(p_payload->>'bot_username',bot_username),owner_lang=coalesce(p_payload->>'owner_lang',owner_lang),status=case when status in('locked','suspended') then status else 'setup' end where id=p_id;
 else raise exception 'INVALID_ACTION';end if;
 update wwc_platform_shops set updated_at=now() where id=p_id returning * into s;
 delete from wwc_platform_events where kind='customer' and created_at<now()-interval '7 days';return to_jsonb(s);
end $$;
commit;
-- Dedicated management bot. Existing store data and NSS balances are not moved.
begin;
create table if not exists public.wwc_manager_users(
 telegram_id bigint primary key check(telegram_id>0),
 language text not null default 'en' check(language in('en','ar','vi','zh')),
 language_chosen boolean not null default false,
 draft_box text,
 draft_expires_at timestamptz,
 updated_at timestamptz not null default now()
);
alter table public.wwc_manager_users enable row level security;
revoke all on public.wwc_manager_users from public,anon,authenticated;
grant all on public.wwc_manager_users to service_role;
create or replace function public.wwc_manager_overview() returns jsonb language sql stable security definer set search_path=public as $$
 select jsonb_build_object('total',count(*),'active',count(*) filter(where status='active'),
 'paid',count(*) filter(where plan='paid' and paid_until>now()),
 'free',count(*) filter(where not(plan='paid' and coalesce(paid_until,'epoch')>now())),
 'other',count(*) filter(where status<>'active')) from wwc_platform_shops
$$;
revoke all on function public.wwc_manager_overview() from public,anon,authenticated;
grant execute on function public.wwc_manager_overview() to service_role;
-- Bound expired secret draft retention even when a user never returns.
create or replace function public.wwc_manager_cleanup() returns void language sql security definer set search_path=public as $$
 update wwc_manager_users set draft_box=null,draft_expires_at=null where draft_expires_at<now() and draft_box is not null
$$;
revoke all on function public.wwc_manager_cleanup() from public,anon,authenticated;
grant execute on function public.wwc_manager_cleanup() to service_role;
commit;
