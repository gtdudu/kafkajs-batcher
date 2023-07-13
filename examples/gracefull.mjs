export function gracefull({ graceMs = 20_000, signals = ['SIGTERM', 'SIGINT'], fn }) {
  let stopping = false;

  signals.forEach((signal) => process.once(signal, handler));

  const timeoutError = new Error(`app took more than ${graceMs}ms to clear event loop.`);
  async function handler(type) {
    if (stopping) {
      return;
    }

    setTimeout(() => {
      throw timeoutError;
    }, graceMs).unref();

    console.error(`${type} event received, initiating clean up, please wait...`);
    stopping = true;

    try {
      await fn();
      process.exitCode = 0;
      stopping = false;
    } catch (e) {
      console.error('GracefullStop error:', e);
      process.exit(1);
    }
  }
}
