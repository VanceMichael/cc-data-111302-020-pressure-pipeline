// 多维资源预算:原始字节、解压字节、待提交记录、待提交字节、打开连接分别计量。
// 每个维度独立排队,先到的等待者先获得预算(FIFO),避免小请求插队让大请求饿死,
// 也保证"谁在等、等了多久、差多少"可以被运行输出观察到。

export class BudgetExhausted extends Error {
  constructor(dim, amount, limit) {
    super(`预算维度 ${dim} 单次需求 ${amount} 超过上限 ${limit}`);
    this.name = 'BudgetExhausted';
    this.dim = dim;
    this.amount = amount;
    this.limit = limit;
  }
}

export class ResourceBudget {
  #limits;
  #used = {};
  #queues = {};
  #stats = {};
  #ownerUsed = new Map();

  constructor(limits) {
    this.#limits = {...limits};
    for (const [dim, limit] of Object.entries(limits)) {
      if (!Number.isFinite(limit) || limit <= 0) throw new Error(`预算上限无效: ${dim}=${limit}`);
      this.#used[dim] = 0;
      this.#queues[dim] = [];
      this.#stats[dim] = {peak: 0, waits: 0, peakWaiters: 0, grants: 0, overdrawn: 0, anomalies: 0};
    }
  }

  limits() {
    return {...this.#limits};
  }

  #check(dim) {
    if (!(dim in this.#limits)) throw new Error(`未知预算维度: ${dim}`);
  }

  #charge(dim, amount, owner) {
    this.#used[dim] += amount;
    const stat = this.#stats[dim];
    stat.grants += 1;
    if (this.#used[dim] > stat.peak) stat.peak = this.#used[dim];
    if (this.#used[dim] > this.#limits[dim]) {
      stat.overdrawn = Math.max(stat.overdrawn, this.#used[dim] - this.#limits[dim]);
    }
    const per = this.#ownerUsed.get(owner) ?? {};
    per[dim] = (per[dim] ?? 0) + amount;
    this.#ownerUsed.set(owner, per);
  }

  // 同步尝试领取;有等待者时不许插队,保持维度内先到先得的顺序。
  tryAcquire(dim, amount, owner = 'unknown') {
    this.#check(dim);
    if (amount > this.#limits[dim]) return false;
    if (this.#queues[dim].length > 0) return false;
    if (this.#used[dim] + amount > this.#limits[dim]) return false;
    this.#charge(dim, amount, owner);
    return true;
  }

  // 领取预算;不足时排队等待。signal 中止时等待立即失败。
  // force 仅用于恢复路径重驱已持久化的批次,允许暂时超限并计入 overdrawn。
  acquire(dim, amount, {owner = 'unknown', signal, force = false} = {}) {
    this.#check(dim);
    if (amount <= 0) return Promise.resolve();
    if (!force && amount > this.#limits[dim]) {
      return Promise.reject(new BudgetExhausted(dim, amount, this.#limits[dim]));
    }
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error('预算领取已取消'));
    if (force) {
      this.#charge(dim, amount, owner);
      return Promise.resolve();
    }
    if (this.#queues[dim].length === 0 && this.#used[dim] + amount <= this.#limits[dim]) {
      this.#charge(dim, amount, owner);
      return Promise.resolve();
    }
    const stat = this.#stats[dim];
    stat.waits += 1;
    const queue = this.#queues[dim];
    return new Promise((resolve, reject) => {
      const waiter = {dim, amount, owner, signal, since: Date.now(), resolve, reject, onAbort: undefined};
      waiter.onAbort = () => {
        const index = queue.indexOf(waiter);
        if (index === -1) return;
        queue.splice(index, 1);
        reject(signal.reason ?? new Error('预算领取已取消'));
      };
      if (signal) signal.addEventListener('abort', waiter.onAbort, {once: true});
      queue.push(waiter);
      if (queue.length > stat.peakWaiters) stat.peakWaiters = queue.length;
    });
  }

  release(dim, amount, owner = 'unknown') {
    this.#check(dim);
    if (amount <= 0) return;
    if (amount > this.#used[dim]) this.#stats[dim].anomalies += 1;
    this.#used[dim] = Math.max(0, this.#used[dim] - amount);
    const per = this.#ownerUsed.get(owner);
    if (per && per[dim] !== undefined) {
      per[dim] -= amount;
      if (per[dim] <= 0) {
        delete per[dim];
        if (Object.keys(per).length === 0) this.#ownerUsed.delete(owner);
      }
    }
    this.#drain(dim);
  }

  // 队头阻塞式授予:只有队头需求能被满足才授予,保证公平;
  // 本链路的请求都是块级大小,队头阻塞不会形成长期卡顿。
  #drain(dim) {
    const queue = this.#queues[dim];
    while (queue.length > 0) {
      const head = queue[0];
      if (this.#used[dim] + head.amount > this.#limits[dim]) break;
      queue.shift();
      if (head.signal) head.signal.removeEventListener('abort', head.onAbort);
      this.#charge(dim, head.amount, head.owner);
      head.resolve();
    }
  }

  // 运行观测:各维度用量、等待者(物主、需求量、已等待时长)、峰值与收紧次数。
  snapshot() {
    const dimensions = {};
    for (const dim of Object.keys(this.#limits)) {
      dimensions[dim] = {
        limit: this.#limits[dim],
        used: this.#used[dim],
        waiting: this.#queues[dim].length,
        waiters: this.#queues[dim].map(w => ({owner: w.owner, amount: w.amount, ms: Date.now() - w.since})),
        ...this.#stats[dim],
      };
    }
    const owners = {};
    for (const [owner, per] of this.#ownerUsed) owners[owner] = {...per};
    return {dimensions, owners};
  }
}
