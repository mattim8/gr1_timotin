```sql

-- 1. Установка расширения pg_cron

create extension if not exists pg_cron;

-- 2. Создание таблиц

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- 3. Функцию логирования изменений по трем полям

create or replace function log_changes()
returns trigger as $$
begin
	IF NEW.name IS DISTINCT FROM OLD.name THEN
	INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
	VALUES(OLD.id, 'name', OLD.name, NEW.name, current_user)
	END IF;

	IF NEW.email IS DISTINCT FROM OLD.email THEN
	INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
	VALUES(OLD.id, 'email', OLD.email, NEW.email, current_user)
	END IF;

	IF NEW.role IS DISTINCT FROM OLD.role THEN
	INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
	VALUES(OLD.id, 'role', OLD.role, NEW.role, current_user)
	END IF;

	RETURN NEW;
end;
$$ LANGUAGE plpgsql;

-- 4. trigger на таблицу users

create or replace trigger trigger_log_user_update;
before update on users
for each row
EXECUTE FUNCTION log_changes();

-- 5. Функцию, которая будет доставать только свежие данные

CREATE OR REPLACE FUNCTION export_todays_audit()
RETURNS void AS $$
DECLARE
    export_path TEXT;
    export_filename TEXT;
BEGIN
   
    export_filename := 'users_audit_export_' || TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '.csv';
    export_path := '/tmp/' || export_filename;
    

    EXECUTE format('COPY (
        SELECT * FROM users_audit 
        WHERE changed_at::date = CURRENT_DATE
        ORDER BY changed_at
    ) TO %L WITH CSV HEADER', export_path);
    
    RAISE NOTICE 'Audit data exported to %', export_path;
END;
$$ LANGUAGE plpgsql;

-- 5. Настройка pg_cron

SELECT cron.schedule(
    'nightly-audit-export',       
    '0 3 * * *',                  
    $$SELECT export_todays_audit()$$
    );
