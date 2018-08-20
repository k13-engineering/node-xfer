const assert = require("assert");
const EventEmitter = require("events");
const equal = require("deep-equal");

const extractKeys = (obj) => {
  let result = [];

  Object.keys(obj).forEach((key) => {
    const value = obj[key];

    if (typeof value === "string") {
      result = result.concat([value]);
    } else if (typeof value === "object") {
      result = result.concat(extractKeys(value));
    }
  });

  return result;
};

const structurize = (def, data) => {
  let result = {};

  Object.keys(def).forEach((key) => {
    const value = def[key];

    if (typeof value === "string") {
      if (typeof data[value] === "undefined") {
        console.log("key = ", key, "data =", data, "value =", value);
        throw new Error("insufficient data supplied");
      }
      result[key] = data[value];
    } else if (typeof value === "object") {
      result[key] = structurize(value, data);
    }
  });

  return result;
}

const destructurize = (def, data) => {
  let result = {};

  Object.keys(data).forEach((key) => {
    if(!Object.keys(def).find((d) => d === key)) {
      throw new Error(`field ${key} not in structure definition`);
    }
  });

  Object.keys(def).forEach((key) => {
    const value = def[key];

    if(typeof data[key] !== "undefined") {
      if (typeof value === "string") {
        result[value] = data[key];
      } else if (typeof value === "object") {
        result = Object.assign({}, result, destructurize(def[key], data[key]));
      }
    }
  });

  return result;
};

const transformer = {
  "create": (defs) => {
    return {
      "required": (jobs) => {
        assert(Array.isArray(jobs), "jobs must be an array");
        assert(jobs.every((job) => typeof job === "string"), "jobs array must only contain strings");

        let result = [];

        jobs.forEach((job) => {
          const def = defs[job];
          if (!def) {
            throw new Error("unknown symbol '" + job + "'");
          } else {
            result = result.concat(extractKeys(def));
          }
        });

        return result;
      },
      "marshal": (jobs) => {
        return destructurize(defs, jobs);
      },
      "unmarshal": (jobs, data) => {
        return jobs.map((job) => {
          const def = defs[job];
          if (!def) {
            throw new Error("unknown symbol '" + job + "'");
          } else {
            return structurize(def, data);
          }
        });
      }
    };
  }
};

const subscription = {
  "emulate": ({ conn, jobs }) => {
    const emitter = new EventEmitter();
    emitter.setMaxListeners(0);

    let closed = false;
    let schedule;

    const next = () => {
      schedule = null;

      conn.read(jobs).then((data) => {
          emitter.emit("data", data);
          schedule = setTimeout(next, 100);
        })
        .catch((err) => {
          if (!closed) {
            emitter.emit("error", err);
          }
        });
    }

    next();

    return {
      "on": emitter.on.bind(emitter),
      "once": emitter.once.bind(emitter),
      "close": () => {
        if (schedule) {
          clearInterval(schedule);
        }
        closed = true;
      }
    };
  },
  "handler": ({ context, jobs }) => {
    const emitter = new EventEmitter();
    emitter.setMaxListeners(0);

    let ctx;
    let closed = false;
    let last = undefined;

    const forward = (ev, data) => {
      if (!closed) {
        if(ev === "data") {
          if(!equal(last, data, {"strict": true})) {
            emitter.emit(ev, data);
            last = data;
          }
        }
        else {
          emitter.emit(ev, data);
        }
      }
    };

    context.aquire()
      .then((conn) => {
        if (closed) {
          conn.release();
          return;
        }

        let sub;

        if (conn.subscribe) {
          sub = conn.subscribe(jobs);
        } else {
          sub = subscription.emulate({ conn, jobs });
        }

        sub.on("data", (data) => forward("data", data));
        sub.on("error", (err) => forward("error", err));

        conn.on("disconnect", (reason) => {
          // console.log("received disconnect from conn");
          sub.close();
          forward("disconnect", reason);
          closed = true;
        });

        ctx = {
          "conn": conn,
          "sub": sub
        };
      })
      .catch((err) => {
        // TODO: ugly event
        forward("disconnect", err);
        closed = true;
      });

    return {
      "on": emitter.on.bind(emitter),
      "once": emitter.once.bind(emitter),
      "close": () => {
        if (closed) {
          return;
        }

        if (ctx) {
          ctx.sub.close();
          ctx.conn.release();
        }

        closed = true;
      }
    };
  }
};

const connectionManager = {
  "create": (connect) => {
    let active = null;
    let emitter = null;

    const register = (handle) => {
      handle.refcount += 1;

      return Object.assign({}, {
        "read": handle.conn.read,
        "write": handle.conn.write,
        "subscribe": handle.conn.subscribe,
        "on": handle.conn.on,
        "once": handle.conn.once,
        "release": () => {
          handle.refcount -= 1;

          if (handle.refcount === 0) {
            handle.conn.close();
          }
        }
      });
    };

    return {
      "aquire": () => {
        if (active) {
          return Promise.resolve(register(active));
        } else {
          if (!emitter) {
            emitter = new EventEmitter();
            emitter.setMaxListeners(0);

            process.nextTick(async() => {
              try {
                const conn = await connect();

                let handle = {
                  "refcount": 0,
                  "conn": conn
                };

                conn.once("disconnect", () => {
                  active = null;
                });

                active = handle;
                emitter.emit("connect", handle);
              } catch (ex) {
                emitter.emit("error", ex);
              } finally {
                emitter = null;
              }
            });
          }

          return new Promise((resolve, reject) => {
            emitter.once("connect", (handle) => {
              resolve(register(handle));
            });
            emitter.once("error", (err) => {
              reject(err);
            });
          });
        }
      }
    };
  }
};


module.exports = {
  "create": (obj) => {
    const connman = connectionManager.create(obj.driver.connect);


    const context = {
      "read": async(jobs) => {
        const conn = await connman.aquire();
        try {
          return conn.read(jobs);
        } finally {
          conn.release();
        }
      },
      "write": async(jobs) => {
        const conn = await connman.aquire();
        try {
          return conn.write(jobs);
        } finally {
          conn.release();
        }
      },
      "aquire": connman.aquire,
      "subscribe": (jobs) => {
        const emitter = new EventEmitter();
        emitter.setMaxListeners(0);

        let handler;

        const resubscribe = () => {
          handler = subscription.handler({ context, jobs });
          handler.on("data", (data) => emitter.emit("data", data));
          handler.on("error", (err) => emitter.emit("error", err));
          handler.on("disconnect", () => {
            emitter.emit("disconnect");
            // TODO: maybe throw error?
            handler.close();
            resubscribe();
          });
        };

        resubscribe();

        return {
          "on": emitter.on.bind(emitter),
          "once": emitter.once.bind(emitter),
          "close": () => handler.close()
        };
      },
      "transform": (defs) => {
        const tf = transformer.create(defs);

        return {
          "read": (jobs) => {
            const required = tf.required(jobs);

            return context.read(required).then((raw) => {
              const data = {};
              required.forEach((key, idx) => {
                data[key] = raw[idx];
              });
              return tf.unmarshal(jobs, data);
            });
          },
          "write": (jobs) => {
            return context.write(tf.marshal(jobs));
          },
          "subscribe": (jobs) => {
            const emitter = new EventEmitter();
            emitter.setMaxListeners(0);

            const required = tf.required(jobs);

            const sub = context.subscribe(required);
            sub.on("data", (raw) => {
              const data = {};
              required.forEach((key, idx) => {
                data[key] = raw[idx];
              });

              try {
                const result = tf.unmarshal(jobs, data);
                emitter.emit("data", result);
              } catch (ex) {
                emitter.emit("error", ex);
              }
            });
            sub.on("error", (err) => {
              emitter.emit("error", err);
            })

            return {
              "on": emitter.on.bind(emitter),
              "once": emitter.once.bind(emitter),
              "close": () => sub.close()
            };
          },
          "close": () => context.close()
        };
      },
      "close": () => {
        if (conn) {
          conn.close();
        }
      }
    };

    return context;
  }
};
