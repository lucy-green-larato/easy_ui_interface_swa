// Local-only shim: on the SWA emulator, reroute '/.auth/me' -> '/api/.auth/me'.
// In Azure, '/.auth/me' remains unchanged.

(function () {
  try {
    const isLocal =
      location.hostname === "127.0.0.1" ||
      location.hostname === "localhost" ||
      location.port === "4280" ||
      location.hostname.endsWith(".github.dev");

    if (!isLocal || !window.fetch) return;

    const origFetch = window.fetch;
    window.fetch = async function (input, init) {
      try {
        let url = "";
        if (typeof input === "string") url = input;
        else if (input && typeof input.url === "string") url = input.url;

        const path = url ? new URL(url, location.origin).pathname : "";
        if (path === "/.auth/me") {
          const opts = Object.assign({ credentials: "include" }, init || {});
          return origFetch("/api/.auth/me", opts);
        }
      } catch { /* ignore */ }
      return origFetch.apply(this, arguments);
    };
  } catch { /* no-op */ }
})();
