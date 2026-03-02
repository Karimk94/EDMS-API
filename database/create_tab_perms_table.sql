-- ============================================================
-- LKP_SEDMS_SECURITY — Per-user sidebar tab permissions
-- Each user has their own row per tab with visibility + read/write.
-- Admin users bypass this table entirely (all access).
-- ============================================================

CREATE TABLE LKP_SEDMS_SECURITY (
    SYSTEM_ID       NUMBER PRIMARY KEY,
    USER_ID         NUMBER NOT NULL,        -- FK to PEOPLE.SYSTEM_ID
    TAB_KEY         VARCHAR2(50) NOT NULL,   -- 'recent', 'favorites', 'folders', 'profilesearch'
    CAN_READ        NUMBER(1) DEFAULT 1,     -- 1 = tab visible, 0 = hidden
    CAN_WRITE       NUMBER(1) DEFAULT 0,     -- 1 = can edit/upload, 0 = read-only
    DISABLED        CHAR(1 BYTE) DEFAULT 'N',
    CONSTRAINT uq_user_tab_perm UNIQUE (USER_ID, TAB_KEY)
);

-- ============================================================
-- No seed data needed — permissions are created automatically
-- when a user is added via the admin panel.
-- Admin users bypass this table entirely in application code.
-- ============================================================

-- ============================================================
-- Backfill: insert default permissions for every existing
-- Smart EDMS user who doesn't have rows yet.
-- 3 rows per user: 'recent', 'folders', 'profilesearch'
-- All visible (CAN_READ=1), read-only (CAN_WRITE=0).
-- ============================================================

INSERT INTO LKP_SEDMS_SECURITY (SYSTEM_ID, USER_ID, TAB_KEY, CAN_READ, CAN_WRITE, DISABLED)
SELECT (SELECT NVL(MAX(SYSTEM_ID), 0) FROM LKP_SEDMS_SECURITY) + ROWNUM,
       u.USER_ID,
       t.TAB_KEY,
       1,           -- CAN_READ  = visible
       0,           -- CAN_WRITE = read-only
       'N'          -- DISABLED  = active
FROM LKP_EDMS_USR_SECUR u
CROSS JOIN (
    SELECT 'recent'        AS TAB_KEY FROM DUAL UNION ALL
    SELECT 'folders'       AS TAB_KEY FROM DUAL UNION ALL
    SELECT 'profilesearch' AS TAB_KEY FROM DUAL
) t
WHERE NOT EXISTS (
    SELECT 1
    FROM LKP_SEDMS_SECURITY s
    WHERE s.USER_ID = u.USER_ID
      AND s.TAB_KEY = t.TAB_KEY
);

COMMIT;
