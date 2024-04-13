const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

class RedisLock {
    constructor(key, lockTimeout, redisInstances) {
        this.key = key;
        this.lockTimeout = lockTimeout;
        this.redisInstances = redisInstances.map(({ host, port }) => new Redis(port, host));
        this.value = uuidv4();
    }

    async acquire() {
        let acquired = 0;
        const startTime = Date.now();

        // Try to acquire the lock in all Redis instances
        for (const redisInstance of this.redisInstances) {
            const result = await redisInstance.set(this.key, this.value, 'PX', this.lockTimeout, 'NX');
            if (result === 'OK') {
                acquired += 1;
            }
        }

        // Check if we have the majority
        if (acquired >= Math.ceil(this.redisInstances.length / 2 + 1)) {
            const endTime = Date.now();
            // Verify the time spent to avoid clock drift issues
            if (endTime - startTime <= this.lockTimeout / 2) {
                return true;
            } else {
                // Release the lock if it took too long to acquire
                await this.release();
                return false;
            }
        } else {
            await this.release();
            return false;
        }
    }

    async release() {
        const script = `
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        `;
        for (const redisInstance of this.redisInstances) {
            await redisInstance.eval(script, 1, this.key, this.value);
        }
    }
}

// Example usage:
const redisServers = [{ host: "192.168.1.1", port: 6379 }, { host: "192.168.1.2", port: 6379 }, { host: "192.168.1.3", port: 6379 }];
const lock = new RedisLock("my_resource_lock", 10000, redisServers);

async function main() {
    if (await lock.acquire()) {
        console.log("Lock acquired!");
        // perform your task
        await lock.release();
    } else {
        console.log("Failed to acquire lock.");
    }
}

main();
