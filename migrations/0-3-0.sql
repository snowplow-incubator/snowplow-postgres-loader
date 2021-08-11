ALTER TABLE public.events
    ALTER COLUMN user_ipaddress TYPE varchar(128),
    ALTER COLUMN domain_userid TYPE varchar(128),
    ALTER COLUMN network_userid TYPE varchar(128),
    ALTER COLUMN refr_domain_userid TYPE varchar(128),
    ALTER COLUMN domain_sessionid TYPE varchar(128);
