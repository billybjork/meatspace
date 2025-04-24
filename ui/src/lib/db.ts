// ui/src/lib/db.ts
import { Pool, PoolClient, QueryResult } from 'pg'; // Import Pool and PoolClient from pg
import dotenv from 'dotenv';
import path from 'path';

// Load environment variables from ui/.env
dotenv.config({ path: path.resolve(process.cwd(), 'ui/.env') });

let pool: Pool | null = null;

/**
 * Initializes and returns the PostgreSQL connection pool singleton.
 * Throws an error if initialization fails.
 */
export function getDbPool(): Pool {
    if (!pool) {
        console.log("Initializing PostgreSQL connection pool...");
        if (!process.env.DATABASE_URL) {
            console.error("FATAL ERROR: DATABASE_URL environment variable not set for Node.js.");
            throw new Error("DATABASE_URL not configured.");
        }

        try {
            pool = new Pool({
                connectionString: process.env.DATABASE_URL,
            });

            // Add listeners for pool events
            pool.on('error', (err, client) => {
                console.error('Unexpected error on idle PostgreSQL client', err);
            });

            pool.on('connect', async (client) => {
                try {
                    const res = await client.query('SELECT pg_backend_pid()');
                    const backendPid = res.rows[0].pg_backend_pid;
                    console.log(`PostgreSQL client connected (Backend PID: ${backendPid})`);
                } catch (err) {
                    console.error('Failed to retrieve backend PID during connect event:', err);
                }
            });

            pool.on('acquire', () => {
                // Optional: log acquired clients
                // console.log(`PostgreSQL client acquired.`);
            });

            pool.on('remove', () => {
                // Optional: log client removal
                // console.log(`PostgreSQL client removed.`);
            });

            console.log("PostgreSQL connection pool created successfully.");
        } catch (error) {
            console.error("FATAL ERROR: Failed to create PostgreSQL pool:", error);
            pool = null;
            throw new Error("Could not create database connection pool.");
        }
    }

    if (!pool) {
        throw new Error("Database pool is not available after initialization attempt.");
    }
    return pool;
}

/**
 * Acquires a client from the pool. Remember to release it!
 * Best used within API route handlers.
 */
export async function getDbClient(): Promise<PoolClient> {
    const currentPool = getDbPool();
    try {
        const client = await currentPool.connect();
        return client;
    } catch (error) {
        console.error("Error acquiring database client from pool:", error);
        throw error;
    }
}

/**
 * Executes a query using a client from the pool.
 * Handles client acquisition and release automatically.
 */
export async function queryDb(text: string, params?: any[]): Promise<QueryResult> {
    const client = await getDbClient();
    try {
        const result = await client.query(text, params);
        return result;
    } finally {
        client.release();
    }
}

/**
 * Closes the database connection pool.
 * Typically called during application shutdown.
 */
export async function closeDbPool(): Promise<void> {
    if (pool) {
        console.log("Closing PostgreSQL connection pool...");
        await pool.end();
        pool = null;
        console.log("PostgreSQL connection pool closed.");
    }
}