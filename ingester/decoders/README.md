https://www.e-navigation.nl/asm?order=field_fi&sort=asc


Type 8 messages have had their 56 bit header bits already stripped and available in the JSON already
Type 6 messages have had their 88 bit header bits already stripped and available in the JSON already

When adding a new decoder ensure to add its description to disseminator/web/asm.json

Get list of received binary messages:


WITH extracted AS (
  SELECT
    id,
    (packet ->> 'MessageID')::INT                             AS message_id,
    (packet -> 'ApplicationID' ->> 'DesignatedAreaCode')::INT AS dac,
    (packet -> 'ApplicationID' ->> 'FunctionIdentifier')::INT  AS fi,
    (packet ->> 'UserID')::INT                                 AS user_id
  FROM messages
  WHERE packet ? 'MessageID'
    AND packet -> 'ApplicationID' ?& ARRAY['DesignatedAreaCode','FunctionIdentifier']
    AND packet ? 'UserID'
),
ranked AS (
  SELECT
    message_id,
    dac,
    fi,
    user_id,
    ROW_NUMBER() OVER (
      PARTITION BY message_id, dac, fi
      ORDER BY id DESC
    ) AS rn
  FROM extracted
)
SELECT
  message_id,
  dac,
  fi,
  -- pick the user_id from the row with rn = 1 (most recent id)
  MAX(CASE WHEN rn = 1 THEN user_id END) AS most_recent_user_id,
  COUNT(*)                                      AS message_count
FROM ranked
GROUP BY
  message_id,
  dac,
  fi
ORDER BY
  message_id,
  dac,
  fi;
