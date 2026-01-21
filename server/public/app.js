import { TREE_ELEMENT_DEFS, loadPluginDefs } from "./tree-elements/elements.js";
import { normalizeElementNode } from "./tree-elements/base.js";

const view = document.getElementById("view");
const title = document.getElementById("view-title");
const statusEl = document.getElementById("server-status");
const navItems = document.querySelectorAll(".nav-item");
const subnavContainers = new Map(
  Array.from(document.querySelectorAll(".subnav")).map((el) => [el.dataset.subnav, el])
);
let showStaleNodes = false;
let activeView = "dashboard";
let refreshToken = 0;
const expandedNodes = new Set();
const expandedLibraries = new Set();
const libraryFilesPage = new Map();
let isLibraryModalOpen = false;
let isLogModalOpen = false;
let isTreeModalOpen = false;
let isTreeAssignModalOpen = false;
let isNodeSettingsModalOpen = false;
let nodeSettingsState = { nodeId: null, name: "", values: {} };
let isTreeDesignerActive = false;
let treeDesignerRoot = null;
let treeDesignerState = { treeId: null };
let appConfig = { debug: false };
let cachedTrees = [];
let pendingTreeId = null;
let treeNodeMenuState = { node: null };
let currentLogJobId = null;
let currentLogEntries = [];
let logPollTimer = null;
let lastLogSnapshot = "";
let lastLogStatus = "";
let openJobActionsId = null;
const collapsedJobSections = new Set();
const jobSectionPage = new Map();
const jobSectionPageSize = new Map();
const expandedNav = new Set();
const libraryFormState = {
  name: "",
  path: "",
  includeExtensions: "",
  excludeExtensions: "",
  nodes: "",
  scanIntervalMin: "",
};

fetchJson("/api/trees/plugins")
  .then((files) => loadPluginDefs(Array.isArray(files) ? files : []))
  .then((plugins) => {
    if (Array.isArray(plugins) && plugins.length) {
      TREE_ELEMENT_DEFS.push(...plugins);
    }
  })
  .catch(() => {});

const routes = {
  dashboard: renderDashboard,
  libraries: renderLibraries,
  nodes: renderNodes,
  jobs: renderJobs,
  trees: renderTrees,
};


navItems.forEach((item) => {
  item.addEventListener("click", (event) => {
    const viewName = item.dataset.view;
    const toggle = event.target.closest("[data-nav-toggle]");
    if (toggle) {
      toggleNav(viewName);
      return;
    }
    setActiveView(viewName, item);
  });
});

document.querySelectorAll(".subnav").forEach((container) => {
  container.addEventListener("click", (event) => {
    const target = event.target.closest("button[data-subnav-view]");
    if (!target) return;
    const viewName = target.dataset.subnavView;
    if (viewName === "trees") {
      pendingTreeId = target.dataset.treeId ?? null;
    }
    setActiveView(viewName);
  });
});

function setActiveView(viewName, clickedItem) {
  navItems.forEach((el) => el.classList.remove("active"));
  if (clickedItem) clickedItem.classList.add("active");
  else {
    document.querySelectorAll(`.nav-item[data-view='${viewName}']`).forEach((el) => {
      el.classList.add("active");
    });
  }
  title.textContent = viewName[0].toUpperCase() + viewName.slice(1);
  activeView = viewName;
  if (viewName !== "trees") {
    isTreeDesignerActive = false;
    treeDesignerState.treeId = null;
  }
  routes[viewName]();
  startAutoRefresh();
}

function toggleNav(viewName) {
  if (expandedNav.has(viewName)) {
    expandedNav.delete(viewName);
  } else {
    expandedNav.add(viewName);
  }
  renderSubnavVisibility();
}

function renderSubnavVisibility() {
  subnavContainers.forEach((container, key) => {
    container.classList.toggle("expanded", expandedNav.has(key));
  });
  document.querySelectorAll(".nav-item[data-expandable='true']").forEach((item) => {
    const viewName = item.dataset.view;
    item.classList.toggle("expanded", expandedNav.has(viewName));
  });
}

async function checkHealth() {
  try {
    const res = await fetchWithTimeout("/api/health", {}, 4000);
    if (!res.ok) throw new Error("Server offline");
    statusEl.textContent = "Server online";
  } catch {
    statusEl.textContent = "Server offline";
  }
}

async function renderDashboard() {
  document.body.classList.add("ui-refreshing");
  const [libs, nodes, stats, jobs] = await Promise.all([
    fetchJson("/api/libraries"),
    fetchJson("/api/nodes"),
    fetchJson("/api/jobs/stats"),
    fetchJson("/api/jobs/queue?limit=500"),
  ]);
  const counts = buildJobCounts(stats);
  const totalFiles = libs.reduce((sum, lib) => sum + Number(lib.file_count ?? 0), 0);
  const avgFilesPerLibrary = libs.length ? Math.round(totalFiles / libs.length) : 0;

  const avgCpu = averageMetric(nodes.map((node) => node.metrics?.cpu?.load));
  const avgRam = averageMetric(nodes.map((node) => node.metrics?.memory?.usedPercent));
  const avgGpu = averageMetric(
    nodes
      .map((node) => averageGpuUtil(mergeGpuLists(node.hardware?.gpus, node.metrics?.gpus)))
      .filter((value) => typeof value === "number")
  );

  const activeTranscodes = jobs.filter(
    (job) => job.type === "transcode" && normalizeJobStatus(job.status) === "processing"
  );
  const avgFps = averageMetric(
    activeTranscodes
      .map((job) => extractFps(job.progress_message))
      .filter((value) => typeof value === "number")
  );

  const totalErrors = counts.healthcheckError + counts.transcodeError;

  view.innerHTML = `
    <div class="card-grid">
      <button class="card card-link" data-card-view="libraries">
        <div class="card-meta">
          <h3>Libraries</h3>
          <span class="muted">${formatCount(libs.length)} configured</span>
        </div>
        <p class="muted">${formatCount(totalFiles)} files total</p>
        <p class="muted">${formatCount(avgFilesPerLibrary)} avg per library</p>
      </button>
      <button class="card card-link" data-card-view="nodes">
        <div class="card-meta">
          <h3>Nodes</h3>
          <span class="muted">${formatCount(nodes.length)} connected</span>
        </div>
        <div class="card-summary">
          ${renderRingGauge(avgCpu, avgRam, avgGpu)}
          <div class="metric-list">
            <div><span>CPU</span>${formatPercent(avgCpu)}</div>
            <div><span>RAM</span>${formatPercent(avgRam)}</div>
            <div><span>GPU</span>${formatPercent(avgGpu)}</div>
          </div>
        </div>
      </button>
      <button class="card card-link" data-card-view="jobs">
        <div class="card-meta">
          <h3>Healthcheck Queue</h3>
          <span class="muted">${formatCount(counts.healthcheckActive)} active</span>
        </div>
        <p class="muted">Errors: ${formatCount(counts.healthcheckError)}</p>
      </button>
      <button class="card card-link" data-card-view="jobs">
        <div class="card-meta">
          <h3>Transcode Queue</h3>
          <span class="muted">${formatCount(counts.transcodeActive)} active</span>
        </div>
        <p class="muted">Successful: ${formatCount(counts.transcodeSuccess)}</p>
        <p class="muted">Failed: ${formatCount(counts.transcodeError)}</p>
      </button>
      <button class="card card-link" data-card-view="jobs">
        <div class="card-meta">
          <h3>Transcode Performance</h3>
          <span class="muted">${formatCount(activeTranscodes.length)} processing</span>
        </div>
        <p class="muted">Avg FPS: ${avgFps != null ? formatFps(avgFps) : "-"}</p>
        <p class="muted">Queue depth: ${formatCount(counts.transcodeActive)}</p>
      </button>
      <button class="card card-link" data-card-view="jobs">
        <div class="card-meta">
          <h3>Error Queue</h3>
          <span class="muted">${formatCount(totalErrors)} total</span>
        </div>
        <p class="muted">Health: ${formatCount(counts.healthcheckError)} • Transcode: ${formatCount(counts.transcodeError)}</p>
      </button>
    </div>
  `;

  bindDashboardCards();
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      document.body.classList.remove("ui-refreshing");
    });
  });
}

function bindDashboardCards() {
  view.querySelectorAll("[data-card-view]").forEach((card) => {
    card.addEventListener("click", () => {
      const viewName = card.dataset.cardView;
      if (viewName) setActiveView(viewName);
    });
  });
}

async function renderLibraries() {
  const existingName = document.querySelector("#library-form input[name='name']")?.value;
  const existingPath = document.querySelector("#library-form input[name='path']")?.value;
  const existingInclude = document.querySelector(
    "#library-form input[name='includeExtensions']"
  )?.value;
  const existingExclude = document.querySelector(
    "#library-form input[name='excludeExtensions']"
  )?.value;
  const existingNodes = document.querySelector("#library-form input[name='nodes']")?.value;
  const existingScan = document.querySelector("#library-form input[name='scanIntervalMin']")?.value;
  if (existingName !== undefined) libraryFormState.name = existingName;
  if (existingPath !== undefined) libraryFormState.path = existingPath;
  if (existingInclude !== undefined) libraryFormState.includeExtensions = existingInclude;
  if (existingExclude !== undefined) libraryFormState.excludeExtensions = existingExclude;
  if (existingNodes !== undefined) libraryFormState.nodes = existingNodes;
  if (existingScan !== undefined) libraryFormState.scanIntervalMin = existingScan;

  const [libs, nodes, trees] = await Promise.all([
    fetchJson("/api/libraries"),
    fetchJson("/api/nodes"),
    fetchJson("/api/trees"),
  ]);
  cachedTrees = Array.isArray(trees) ? trees : [];
  const nodeNames = nodes.map((node) => node.name).sort();

  view.innerHTML = `
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Libraries</h3>
          <p class="muted">Manage library entries and scan settings.</p>
        </div>
        <button class="primary" id="add-library">Add Library</button>
      </div>
    </div>
    <div class="card" style="margin-top:16px;">
      <h3>Configured Libraries</h3>
      <table class="table">
        <thead>
          <tr><th>Name</th><th>Path</th><th>Files</th><th>Added</th><th></th></tr>
        </thead>
        <tbody>
          ${libs
            .map((lib) => {
              const isExpanded = expandedLibraries.has(lib.id);
              const detailsRow = renderLibraryDetailsRow(lib, isExpanded);
              return `
                  <tr>
                    <td>${lib.name}</td>
                    <td>${lib.path}</td>
                    <td>${lib.file_count ?? 0}</td>
                    <td>${new Date(lib.created_at).toLocaleString()}</td>
                    <td>
                      <button class="ghost" data-library-toggle="${lib.id}">Files</button>
                      <button class="ghost" data-library-rescan="${lib.id}">Rescan</button>
                      <button class="ghost" data-library-edit="${lib.id}">Edit</button>
                      <button class="ghost danger" data-library-delete="${lib.id}">Delete</button>
                    </td>
                  </tr>
                  ${detailsRow}
                `;
            })
            .join("")}
        </tbody>
      </table>
    </div>
    ${renderLibraryModal(nodeNames, cachedTrees)}
  `;

  const addBtn = document.getElementById("add-library");
  addBtn.addEventListener("click", () => openLibraryModal());

  const toggleButtons = view.querySelectorAll("button[data-library-toggle]");
  toggleButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const libId = btn.dataset.libraryToggle;
      if (expandedLibraries.has(libId)) {
        expandedLibraries.delete(libId);
      } else {
        expandedLibraries.add(libId);
      }
      await renderLibraries();
    });
  });

  const editButtons = view.querySelectorAll("button[data-library-edit]");
  editButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const libId = btn.dataset.libraryEdit;
      const lib = libs.find((item) => item.id === libId);
      openLibraryModal(lib);
    });
  });

  const rescanButtons = view.querySelectorAll("button[data-library-rescan]");
  rescanButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const libId = btn.dataset.libraryRescan;
      if (!libId) return;
      await fetch(`/api/libraries/${libId}/rescan`, { method: "POST" });
      await renderLibraries();
    });
  });

  const deleteButtons = view.querySelectorAll("button[data-library-delete]");
  deleteButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const libId = btn.dataset.libraryDelete;
      const lib = libs.find((item) => item.id === libId);
      const name = lib?.name ?? "this library";
      if (!confirm(`Delete ${name}? This will remove its jobs and files.`)) return;
      await fetch(`/api/libraries/${libId}`, { method: "DELETE" });
      expandedLibraries.delete(libId);
      await renderLibraries();
    });
  });

  bindLibraryModal(libs);
}

async function renderNodes() {
  const nodes = await fetchJson(`/api/nodes${showStaleNodes ? "?include_stale=1" : ""}`);

  view.innerHTML = `
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Connected Nodes</h3>
          <p class="muted">${showStaleNodes ? "Showing active + stale" : "Showing active only"}</p>
        </div>
        <label class="toggle">
          <input type="checkbox" id="toggle-stale" ${showStaleNodes ? "checked" : ""} />
          <span>Show stale</span>
        </label>
      </div>
      <table class="table">
        <thead>
          <tr><th>Name</th><th>Status</th><th>Platform</th><th>CPU</th><th>RAM</th><th>GPU</th><th></th></tr>
        </thead>
        <tbody>
          ${nodes
            .map((node) => {
              const cpuValue = node.metrics?.cpu?.load;
              const cpuText = typeof cpuValue === "number" ? `${cpuValue.toFixed(1)}%` : "-";
              const ramPercent = node.metrics?.memory?.usedPercent;
              const ramUsed = node.metrics?.memory?.usedBytes;
              const ramTotal = node.hardware?.memory?.totalBytes ?? node.metrics?.memory?.totalBytes;
              const ramDetailText = formatRamUsage(ramUsed, ramTotal);
              const ramText =
                typeof ramPercent === "number"
                  ? `${ramPercent.toFixed(1)}% (${ramDetailText})`
                  : "-";

              const gpus = mergeGpuLists(node.hardware?.gpus, node.metrics?.gpus);
              const gpuUtil = averageGpuUtil(gpus);
              const gpuSummary =
                typeof gpuUtil === "number" ? `${gpuUtil.toFixed(1)}% (${gpus.length})` : "-";
              const status = node.stale ? "stale" : "active";
              const staleClass = node.stale ? "stale" : "";
              const isExpanded = expandedNodes.has(node.id);
              const detailsRow = renderGpuDetailsRow(
                node.id,
                gpus,
                isExpanded,
                node.last_seen,
                ramDetailText
              );

              return `<tr>
                <td>${node.name}</td>
                <td><span class="status-pill ${staleClass}">${status}</span></td>
                <td>${node.platform}</td>
                <td class="usage-cell">${renderUsageMeter(cpuValue, cpuText)}</td>
                <td class="usage-cell">${renderUsageMeter(ramPercent, ramText, true)}</td>
                <td class="usage-cell">${renderUsageMeter(gpuUtil, gpuSummary)}</td>
                <td>
                  ${
                    node.stale
                      ? `<button class="danger" data-node-id="${node.id}">Delete</button>`
                      : `<button class="ghost" data-gpu-toggle="${node.id}">Details</button>`
                  }
                  <button class="ghost" data-node-limits="${node.id}">Limits</button>
                </td>
              </tr>${detailsRow}`;
            })
            .join("")}
        </tbody>
      </table>
    </div>
    ${renderNodeSettingsModal()}
  `;

  const toggle = document.getElementById("toggle-stale");
  toggle.addEventListener("change", async () => {
    showStaleNodes = toggle.checked;
    await renderNodes();
  });

  const deleteButtons = view.querySelectorAll("button[data-node-id]");
  deleteButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nodeId = btn.dataset.nodeId;
      await fetch(`/api/nodes/${encodeURIComponent(nodeId)}`, { method: "DELETE" });
      await renderNodes();
    });
  });


  const limitsButtons = view.querySelectorAll("button[data-node-limits]");
  limitsButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const nodeId = button.dataset.nodeLimits;
      const node = nodes.find((item) => item.id === nodeId);
      openNodeSettingsModal(node);
    });
  });

  bindNodeSettingsModal();
  const gpuButtons = view.querySelectorAll("button[data-gpu-toggle]");
  gpuButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nodeId = btn.dataset.gpuToggle;
      if (expandedNodes.has(nodeId)) {
        expandedNodes.delete(nodeId);
      } else {
        expandedNodes.add(nodeId);
      }
      await renderNodes();
    });
  });
}

async function renderJobs() {
  const [jobs, config] = await Promise.all([
    fetchJson("/api/jobs/queue?limit=1000"),
    fetchJson("/api/config"),
  ]);

  const isActiveStatus = (status) => status === "queued" || status === "processing";
  const sections = [
    {
      title: "Healthcheck",
      key: "healthcheck",
      filter: (job) => job.type === "healthcheck" && isActiveStatus(normalizeJobStatus(job.status)),
    },
    {
      title: "Health Failed",
      key: "health_failed",
      filter: (job) => job.type === "healthcheck" && normalizeJobStatus(job.status) === "error",
    },
    {
      title: "Transcode",
      key: "transcode",
      filter: (job) => job.type === "transcode" && isActiveStatus(normalizeJobStatus(job.status)),
    },
    {
      title: "Transcode Successful",
      key: "transcode_successful",
      filter: (job) => job.type === "transcode" && normalizeJobStatus(job.status) === "successful",
    },
    {
      title: "Failed Transcode",
      key: "transcode_failed",
      filter: (job) => job.type === "transcode" && normalizeJobStatus(job.status) === "error",
    },
  ];

  view.innerHTML = `
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Recent Jobs</h3>
          <p class="muted">Re-enqueue failed or unhealthy jobs.</p>
        </div>
        <div class="actions">
          <button class="ghost" id="reenqueue-health-failed">Re-enqueue health failed</button>
          <button class="ghost" id="reenqueue-transcode-failed">Re-enqueue failed transcodes</button>
          ${
            config.debug
              ? `<button class="danger" id="reset-jobs">Reset jobs</button>`
              : ""
          }
        </div>
      </div>
      ${sections
        .map((section) => {
          const rows = jobs.filter(section.filter);
          const isCollapsed = collapsedJobSections.has(section.key);
          const pageSize = jobSectionPageSize.get(section.key) ?? 25;
          const page = jobSectionPage.get(section.key) ?? 0;
          const totalPages = pageSize === Infinity ? 1 : Math.max(Math.ceil(rows.length / pageSize), 1);
          const start = pageSize === Infinity ? 0 : page * pageSize;
          const visibleRows = pageSize === Infinity ? rows : rows.slice(start, start + pageSize);
          return `
            <div class="card" style="margin-top:16px;">
              <div class="card-header">
                <h4>${section.title} <span class="muted">(${rows.length})</span></h4>
                <button class="ghost" data-job-section-toggle="${section.key}">
                  ${isCollapsed ? "Expand" : "Collapse"}
                </button>
              </div>
              <div class="jobs-section-controls ${isCollapsed ? "hidden" : ""}">
                <label class="muted">Show</label>
                <select data-job-page-size="${section.key}">
                  ${[10, 25, 50, 100, "all"]
                    .map((size) => {
                      const value = size === "all" ? "all" : String(size);
                      const selected =
                        (size === "all" && pageSize === Infinity) || pageSize === Number(size);
                      return `<option value="${value}" ${selected ? "selected" : ""}>${size}</option>`;
                    })
                    .join("")}
                </select>
                <div class="pager">
                  <button class="ghost" data-job-page-prev="${section.key}" ${
                    page === 0 || pageSize === Infinity ? "disabled" : ""
                  }>Prev</button>
                  <span class="muted">Page ${page + 1} / ${totalPages}</span>
                  <button class="ghost" data-job-page-next="${section.key}" ${
                    page + 1 >= totalPages || pageSize === Infinity ? "disabled" : ""
                  }>Next</button>
                </div>
              </div>
              <table class="table ${isCollapsed ? "hidden" : ""}">
                <thead>
                  <tr><th>Job</th><th>Type</th><th>Status</th><th>Progress</th><th>Node</th><th>Updated</th><th></th></tr>
                </thead>
                <tbody>
                  ${visibleRows
                    .map((job) => {
                      const normalizedStatus = normalizeJobStatus(job.status);
                      const canReenqueue = normalizedStatus === "error";
                      const isTranscodeSuccess =
                        normalizedStatus === "successful" && job.type === "transcode";
                      const typeText = job.type ?? "-";
                      const fileName = getFileName(job.file_path ?? job.new_path ?? "");
                      const jobTitle = fileName || job.file_id || job.id;
                      return `
                        <tr>
                          <td>
                            <div class="job-title">${jobTitle}</div>
                            <div class="muted">${job.id}</div>
                          </td>
                          <td>${typeText}</td>
                          <td>${normalizedStatus}</td>
                          <td>${formatJobProgress(job)}</td>
                          <td>${job.assigned_node_id ?? "-"}</td>
                          <td>${new Date(job.updated_at).toLocaleString()}</td>
                          <td>
                            <div class="job-actions">
                              ${
                                canReenqueue
                                  ? `<button class="ghost" data-reenqueue-status="${normalizedStatus}" data-reenqueue-type="${job.type}">Re-enqueue</button>`
                                  : ""
                              }
                              ${
                                isTranscodeSuccess
                                  ? `
                                    <div class="job-action-wrap">
                                      <button class="icon-button" data-job-actions="${job.id}" aria-label="Actions" title="Actions">
                                        <svg viewBox="0 0 24 24" aria-hidden="true">
                                          <path d="M19.4 13.5c.04-.5.04-1 .02-1.5l2-1.6c.18-.14.23-.4.12-.6l-1.9-3.3c-.11-.2-.36-.28-.57-.2l-2.3.9c-.4-.3-.9-.6-1.4-.8l-.3-2.4a.48.48 0 0 0-.48-.42h-3.8c-.24 0-.45.18-.48.42l-.3 2.4c-.5.2-1 .5-1.4.8l-2.3-.9c-.21-.08-.46 0-.57.2L2.7 9.8c-.11.2-.06.46.12.6l2 1.6c-.02.5-.02 1 .02 1.5l-2 1.6c-.18.14-.23.4-.12.6l1.9 3.3c.11.2.36.28.57.2l2.3-.9c.4.3.9.6 1.4.8l.3 2.4c.03.24.24.42.48.42h3.8c.24 0 .45-.18.48-.42l.3-2.4c.5-.2 1-.5 1.4-.8l2.3.9c.21.08.46 0 .57-.2l1.9-3.3c.11-.2.06-.46-.12-.6l-2-1.6ZM12 15.6a3.6 3.6 0 1 1 0-7.2 3.6 3.6 0 0 1 0 7.2Z"/>
                                        </svg>
                                        <span class="icon-label">Actions</span>
                                      </button>
                                      <div class="job-action-menu hidden" data-job-action-menu="${job.id}">
                                        <button class="job-action-item" data-action-item="healthcheck" data-job-id="${job.id}">Send to healthcheck</button>
                                        <button class="job-action-item" data-action-item="transcode" data-job-id="${job.id}">Send to transcode</button>
                                      </div>
                                    </div>
                                  `
                                  : ""
                              }
                              <button class="icon-button" data-job-logs="${job.id}" aria-label="Logs" title="Logs">
                                <svg viewBox="0 0 24 24" aria-hidden="true">
                                  <path d="M6 2h8l4 4v16H6V2Zm8 1.5V7h3.5L14 3.5ZM8.5 10h7v1.6h-7V10Zm0 3h7v1.6h-7V13Zm0 3h5.2v1.6H8.5V16Z"/>
                                </svg>
                                <span class="icon-label">Logs</span>
                              </button>
                            </div>
                          </td>
                        </tr>
                      `;
                    })
                    .join("")}
                </tbody>
              </table>
            </div>
          `;
        })
        .join("")}
    </div>
    ${renderLogModal()}
  `;

  const sectionToggles = view.querySelectorAll("button[data-job-section-toggle]");
  sectionToggles.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.dataset.jobSectionToggle;
      if (collapsedJobSections.has(key)) {
        collapsedJobSections.delete(key);
      } else {
        collapsedJobSections.add(key);
      }
      await renderJobs();
    });
  });

  const pageSizeSelects = view.querySelectorAll("select[data-job-page-size]");
  pageSizeSelects.forEach((select) => {
    select.addEventListener("change", async () => {
      const key = select.dataset.jobPageSize;
      const value = select.value;
      const size = value === "all" ? Infinity : Number(value);
      jobSectionPageSize.set(key, size);
      jobSectionPage.set(key, 0);
      await renderJobs();
    });
  });

  const prevButtons = view.querySelectorAll("button[data-job-page-prev]");
  prevButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.dataset.jobPagePrev;
      const page = jobSectionPage.get(key) ?? 0;
      jobSectionPage.set(key, Math.max(page - 1, 0));
      await renderJobs();
    });
  });

  const nextButtons = view.querySelectorAll("button[data-job-page-next]");
  nextButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.dataset.jobPageNext;
      const page = jobSectionPage.get(key) ?? 0;
      jobSectionPage.set(key, page + 1);
      await renderJobs();
    });
  });

  const reenqueueHealthFailed = document.getElementById("reenqueue-health-failed");
  const reenqueueFailed = document.getElementById("reenqueue-transcode-failed");
  const resetJobs = document.getElementById("reset-jobs");

  reenqueueHealthFailed?.addEventListener("click", async () => {
    await fetch("/api/jobs/reenqueue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "error", type: "healthcheck" }),
    });
    await renderJobs();
  });

  reenqueueFailed?.addEventListener("click", async () => {
    await fetch("/api/jobs/reenqueue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "error", type: "transcode" }),
    });
    await renderJobs();
  });

  resetJobs?.addEventListener("click", async () => {
    if (!confirm("This will clear all jobs and reset file statuses. Continue?")) return;
    await fetch("/api/jobs/reset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    await renderJobs();
  });

  const reenqueueButtons = view.querySelectorAll("button[data-reenqueue-status]");
  reenqueueButtons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const status = btn.dataset.reenqueueStatus;
      const type = btn.dataset.reenqueueType;
      await fetch("/api/jobs/reenqueue", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status, type }),
      });
      await renderJobs();
    });
  });

  if (openJobActionsId) {
    const menu = view.querySelector(`[data-job-action-menu='${openJobActionsId}']`);
    if (menu) menu.classList.remove("hidden");
  }

  const actionButtons = view.querySelectorAll("button[data-job-actions]");
  actionButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const jobId = button.dataset.jobActions;
      if (!jobId) return;
      const menu = view.querySelector(`[data-job-action-menu='${jobId}']`);
      if (!menu) return;
      const isHidden = menu.classList.contains("hidden");
      view.querySelectorAll(".job-action-menu").forEach((item) => item.classList.add("hidden"));
      menu.classList.toggle("hidden", !isHidden);
      openJobActionsId = isHidden ? jobId : null;
    });
  });

  view.addEventListener("click", (event) => {
    const target = event.target.closest("button[data-action-item]");
    if (!target) {
      view.querySelectorAll(".job-action-menu").forEach((item) => item.classList.add("hidden"));
      openJobActionsId = null;
      return;
    }
    const jobId = target.dataset.jobId;
    const targetType = target.dataset.actionItem;
    if (!jobId || !targetType) return;
    fetch("/api/jobs/requeue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jobId, targetType }),
    }).then(() => {
      openJobActionsId = null;
      renderJobs();
    });
  });

  const logButtons = view.querySelectorAll("button[data-job-logs]");
  logButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const jobId = btn.dataset.jobLogs;
      const job = jobs.find((item) => String(item.id) === String(jobId));
      if (job) openLogModal(job);
    });
  });

  bindLogModal();
}

async function renderTrees() {
  isTreeDesignerActive = false;
  treeDesignerRoot = null;
  const trees = await fetchJson("/api/trees");

  view.innerHTML = `
    <div class="trees-topbar">
      <div>
        <h3>Trees</h3>
        <p class="muted">Select a tree from the sidebar to edit.</p>
      </div>
      <button class="primary" id="tree-create">Create Tree</button>
    </div>
    <div class="card">
      <div class="card-header">
        <div>
          <h3>Tree Designer</h3>
          <p class="muted">Drag nodes and connect branches. Save to create a new version.</p>
        </div>
        <div class="actions">
          <button class="ghost" id="tree-designer-save" disabled>Save</button>
          <select id="tree-actions" class="tree-actions">
            <option value="">Actions</option>
            <option value="edit">Edit tree settings</option>
            <option value="delete">Delete tree</option>
          </select>
        </div>
      </div>
      <div id="tree-designer" class="tree-designer">
        <div class="muted">Select a tree from the sidebar to start designing.</div>
      </div>
    </div>
    ${renderTreeModal()}
    ${renderTreeSettingsModal()}
  `;

  document.getElementById("tree-create")?.addEventListener("click", () => {
    openTreeModal();
  });

  bindTreeModal();
  bindTreeSettingsModal();

  document.getElementById("tree-designer-save")?.addEventListener("click", async () => {
    if (typeof treeDesignerState.save === "function") {
      await treeDesignerState.save();
    }
  });

  document.getElementById("tree-actions")?.addEventListener("change", async (event) => {
    const action = event.target.value;
    event.target.value = "";
    if (action === "edit") {
      const treeId = treeDesignerState.treeId;
      if (!treeId) return;
      const tree = await fetchJson(`/api/trees/${treeId}`);
      openTreeModal(tree);
      return;
    }
    if (action === "delete") {
      const treeId = treeDesignerState.treeId;
      if (!treeId) return;
      if (!confirm("Delete this tree? This cannot be undone.")) return;
      await fetch(`/api/trees/${treeId}`, { method: "DELETE" });
      treeDesignerState.treeId = null;
      await renderTrees();
    }
  });

  if (pendingTreeId) {
    const tree = await fetchJson(`/api/trees/${pendingTreeId}`);
    pendingTreeId = null;
    mountTreeDesigner(tree);
  }
}

async function fetchJson(url, { timeoutMs = 8000 } = {}) {
  const res = await fetchWithTimeout(url, {}, timeoutMs);
  if (!res.ok) throw new Error("Request failed");
  return res.json();
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  if (typeof AbortController === "undefined") {
    return fetch(url, options);
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function loadAppConfig() {
  try {
    appConfig = await fetchJson("/api/config");
  } catch {
    appConfig = appConfig ?? { debug: false };
  }
}

function showLoadError(message) {
  if (!view) return;
  view.innerHTML = `
    <div class="card">
      <h3>Unable to load data</h3>
      <p class="muted">${message}</p>
    </div>
  `;
}

function formatBytes(value) {
  if (!value || Number.isNaN(value)) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

function formatBytesDecimal(value) {
  if (!value || Number.isNaN(value)) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1000 && unitIndex < units.length - 1) {
    size /= 1000;
    unitIndex += 1;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

function formatBytesBinary(value) {
  if (!value || Number.isNaN(value)) return "-";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)}${units[unitIndex]}`;
}

function formatSubtitleList(value) {
  if (!value) return "-";
  if (Array.isArray(value)) return value.length ? value.join(",") : "none";
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.length ? parsed.join(",") : "none";
    } catch {
      // ignore
    }
    return value.trim() || "-";
  }
  return "-";
}

function formatDurationSeconds(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return "-";
  const total = Math.round(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  const padded = (num) => String(num).padStart(2, "0");
  return hours > 0
    ? `${hours}:${padded(minutes)}:${padded(secs)}`
    : `${minutes}:${padded(secs)}`;
}

function formatFrameCount(value) {
  const count = Number(value);
  if (!Number.isFinite(count) || count <= 0) return "-";
  return `${Math.round(count)}f`;
}

function normalizeContainerDisplay(value) {
  const name = String(value ?? "").toLowerCase();
  if (!name || name === "-") return "-";
  if (name === "matroska") return "mkv";
  if (name === "quicktime") return "mov";
  return name;
}

function formatRamUsage(usedBytes, totalBytes) {
  if (!totalBytes || Number.isNaN(totalBytes)) return "-";
  const gib = 1024 ** 3;
  const mib = 1024 ** 2;
  if (totalBytes < gib) {
    const totalMiB = Math.max(1, Math.round(totalBytes / mib));
    const usedMiB = usedBytes ? (usedBytes / mib).toFixed(1) : "0.0";
    return `${usedMiB}MiB/${totalMiB}MiB`;
  }
  const totalGiB = Math.max(1, Math.round(totalBytes / gib));
  const usedGiB = usedBytes ? (usedBytes / gib).toFixed(1) : "0.0";
  return `${usedGiB}GiB/${totalGiB}GiB`;
}

function getFileName(filePath) {
  if (!filePath) return "";
  const parts = String(filePath).split(/[/\\]/);
  return parts[parts.length - 1] ?? "";
}

function mergeGpuLists(hardwareGpus = [], metricGpus = []) {
  const merged = [...metricGpus.map((gpu) => ({ ...gpu }))];

  hardwareGpus.forEach((gpu) => {
    const match = merged.find((candidate) => matchesGpu(candidate, gpu));
    if (!match) {
      merged.push({ ...gpu });
    } else {
      match.model = match.model ?? gpu.model;
      match.vendor = match.vendor ?? gpu.vendor;
      match.vram = match.vram ?? gpu.vram;
      match.driver = match.driver ?? gpu.driver;
      match.accelerator = match.accelerator ?? gpu.accelerator;
    }
  });

  return merged;
}

function matchesGpu(a, b) {
  const aModel = String(a.model ?? "").toLowerCase();
  const bModel = String(b.model ?? "").toLowerCase();
  const aVendor = String(a.vendor ?? "").toLowerCase();
  const bVendor = String(b.vendor ?? "").toLowerCase();
  if (aModel && bModel) {
    return aModel.includes(bModel) || bModel.includes(aModel);
  }
  return aVendor && bVendor && aVendor.includes(bVendor);
}

function averageGpuUtil(gpus) {
  const utilizations = gpus
    .filter((gpu) => gpu.accelerator !== false)
    .map((gpu) => gpu.utilization)
    .filter((value) => typeof value === "number");
  if (!utilizations.length) return null;
  const total = utilizations.reduce((sum, value) => sum + value, 0);
  return total / utilizations.length;
}

function renderUsageMeter(value, text, hideText = false) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return `<div class="usage-meter" title="${text ?? "-"}"><div class="usage-text">${hideText ? "-" : text ?? "-"}</div></div>`;
  }
  const clamped = Math.max(0, Math.min(100, value));
  const display = hideText ? `${clamped.toFixed(1)}%` : text ?? `${clamped.toFixed(1)}%`;
  return `
    <div class="usage-meter" title="${text ?? display}">
      <div class="usage-text">${display}</div>
      <div class="usage-bar"><div class="usage-fill" style="width:${clamped}%;"></div></div>
    </div>
  `;
}

function formatJobProgress(job) {
  const progressValue = Number(job?.progress ?? 0);
  const percent = Number.isFinite(progressValue) ? Math.round(progressValue) : 0;
  const fps = extractFps(job?.progress_message);
  if (fps != null) return `${percent}% • ${formatFps(fps)} fps`;
  return `${percent}%`;
}

function extractFps(message) {
  if (!message) return null;
  const match = String(message).match(/fps=\s*([0-9.]+)/i);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function formatFps(value) {
  const rounded = Math.round(value * 10) / 10;
  if (Number.isNaN(rounded)) return "-";
  return rounded % 1 === 0 ? String(rounded.toFixed(0)) : String(rounded);
}

function averageMetric(values) {
  const cleaned = (values ?? []).filter((value) => typeof value === "number" && !Number.isNaN(value));
  if (!cleaned.length) return null;
  const total = cleaned.reduce((sum, value) => sum + value, 0);
  return total / cleaned.length;
}

function formatCount(value) {
  const numberValue = Number(value ?? 0);
  if (!Number.isFinite(numberValue)) return "-";
  return new Intl.NumberFormat().format(numberValue);
}

function formatPercent(value) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  const rounded = Math.round(value * 10) / 10;
  return rounded % 1 === 0 ? `${rounded.toFixed(0)}%` : `${rounded.toFixed(1)}%`;
}

function clampPercent(value) {
  if (typeof value !== "number" || Number.isNaN(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function renderRingGauge(cpu, ram, gpu) {
  const cpuValue = clampPercent(cpu);
  const ramValue = clampPercent(ram);
  const gpuValue = clampPercent(gpu);
  const ariaLabel = `CPU ${formatPercent(cpu)}, RAM ${formatPercent(ram)}, GPU ${formatPercent(gpu)}`;
  return `
    <div class="ring-gauge" role="img" aria-label="${ariaLabel}">
      <span class="ring" style="--value:${cpuValue}; --ring-color:#6d7dff; --thickness:8px; --inset:0px;"></span>
      <span class="ring" style="--value:${ramValue}; --ring-color:#2fe3bb; --thickness:8px; --inset:10px;"></span>
      <span class="ring" style="--value:${gpuValue}; --ring-color:#ffcc70; --thickness:8px; --inset:20px;"></span>
      <span class="ring-center">AVG</span>
    </div>
  `;
}

function normalizeJobStatus(status) {
  if (status === "health_failed" || status === "transcode_failed") return "error";
  if (status === "transcode_successful") return "successful";
  return status ?? "-";
}

function buildJobCounts(stats) {
  const isActiveStatus = (status) => status === "queued" || status === "processing";
  const countBy = (type, predicate) =>
    stats
      .filter((row) => row.type === type)
      .filter((row) => predicate(normalizeJobStatus(row.status)))
      .reduce((sum, row) => sum + Number(row.count ?? 0), 0);

  return {
    healthcheckActive: countBy("healthcheck", isActiveStatus),
    healthcheckError: countBy("healthcheck", (s) => s === "error"),
    transcodeActive: countBy("transcode", isActiveStatus),
    transcodeSuccess: countBy("transcode", (s) => s === "successful"),
    transcodeError: countBy("transcode", (s) => s === "error"),
  };
}


function renderGpuDetailsRow(nodeId, gpus, expanded, lastSeen, ramDetailText) {
  if (!expanded) {
    return `<tr class="gpu-details hidden" data-gpu-details="${nodeId}"></tr>`;
  }

  if (!gpus.length) {
    return `
      <tr class="gpu-details" data-gpu-details="${nodeId}">
        <td colspan="8"><span class="muted">No GPUs reported.</span></td>
      </tr>
    `;
  }

  const rows = gpus
    .map((gpu) => {
      const util = typeof gpu.utilization === "number" ? `${gpu.utilization.toFixed(1)}%` : "-";
      const vramTotalBytes = gpu.vram ? gpu.vram * 1024 * 1024 : null;
      const vramUsedBytes =
        typeof gpu.memoryUtilization === "number" && vramTotalBytes
          ? (gpu.memoryUtilization / 100) * vramTotalBytes
          : null;
      const vramText = vramTotalBytes
        ? `${formatBytes(vramUsedBytes ?? 0)}/${formatBytes(vramTotalBytes)}`
        : "-";
      const vramPercent = typeof gpu.memoryUtilization === "number" ? gpu.memoryUtilization : null;
      const accel = gpu.accelerator === false ? "integrated" : "accelerator";
      return `
        <tr>
          <td>${gpu.model ?? "Unknown"}</td>
          <td>${gpu.vendor ?? "-"}</td>
          <td>${accel}</td>
          <td>${util}</td>
          <td>${renderUsageMeter(vramPercent, vramText)}</td>
        </tr>
      `;
    })
    .join("");

  return `
    <tr class="gpu-details" data-gpu-details="${nodeId}">
      <td colspan="8">
        <div class="gpu-details-meta">
          <div><span class="muted">Last seen:</span> ${new Date(lastSeen).toLocaleString()}</div>
          <div><span class="muted">RAM:</span> ${ramDetailText}</div>
        </div>
        <table class="table subtable">
          <thead>
            <tr><th>Model</th><th>Vendor</th><th>Type</th><th>GPU</th><th>VRAM Usage</th></tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </td>
    </tr>
  `;
}

async function startAutoRefresh() {
  const token = ++refreshToken;
  const waitMs = 1500;

  while (token === refreshToken) {
    try {
      if (!isUserEditing()) {
        if (activeView === "trees") {
          await renderSidebarLists();
          await new Promise((resolve) => setTimeout(resolve, waitMs));
          continue;
        }
        const scrollTop = view.scrollTop;
        await renderSidebarLists();
        await routes[activeView]();
        view.scrollTop = scrollTop;
      }
    } catch (err) {
      console.warn("Auto-refresh failed:", err.message ?? err);
    }

    await new Promise((resolve) => setTimeout(resolve, waitMs));
  }
}

async function renderSidebarLists() {
  try {
    const [libraries, nodes, trees] = await Promise.all([
      fetchJson("/api/libraries"),
      fetchJson("/api/nodes"),
      fetchJson("/api/trees"),
    ]);

    const libsContainer = subnavContainers.get("libraries");
    if (libsContainer) {
      libsContainer.innerHTML = libraries
        .map(
          (lib) =>
            `<button class="subnav-item" data-subnav-view="libraries">${lib.name}</button>`
        )
        .join("");
    }

    const nodesContainer = subnavContainers.get("nodes");
    if (nodesContainer) {
      nodesContainer.innerHTML = nodes
        .map(
          (node) =>
            `<button class="subnav-item" data-subnav-view="nodes">${node.name}</button>`
        )
        .join("");
    }

    const jobsContainer = subnavContainers.get("jobs");
    if (jobsContainer) {
      const queues = [
        "Healthcheck",
        "Health Failed",
        "Transcode",
        "Transcode Successful",
        "Transcode Failed",
      ];
      jobsContainer.innerHTML = queues
        .map((queue) => `<button class="subnav-item" data-subnav-view="jobs">${queue}</button>`)
        .join("");
    }

    const treesContainer = subnavContainers.get("trees");
    if (treesContainer) {
      treesContainer.innerHTML = trees
        .map(
          (tree) =>
            `<button class="subnav-item" data-subnav-view="trees" data-tree-id="${tree.id}">${tree.name}</button>`
        )
        .join("");
    }
  } catch {
    // ignore sidebar refresh errors
  }
}

function isUserEditing() {
  const active = document.activeElement;
  if (!active) return false;
  if (
    isLibraryModalOpen ||
    isLogModalOpen ||
    isTreeModalOpen ||
    isTreeAssignModalOpen ||
    isNodeSettingsModalOpen ||
    isTreeDesignerActive
  ) {
    return true;
  }
  const tag = active.tagName?.toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") {
    return view.contains(active);
  }
  return false;
}

function renderLibraryDetailsRow(lib, expanded) {
  if (!expanded) {
    return `<tr class="library-details hidden" data-library-details="${lib.id}"></tr>`;
  }

  const page = libraryFilesPage.get(lib.id) ?? 0;
  const offset = page * 25;
  return `
    <tr class="library-details" data-library-details="${lib.id}">
      <td colspan="5">
        <div class="library-files" data-library-files="${lib.id}" data-offset="${offset}">
          <div class="muted">Loading files...</div>
        </div>
      </td>
    </tr>
  `;
}

async function renderLibraryFiles(libId, offset) {
  const container = view.querySelector(`[data-library-files='${libId}']`);
  if (!container) return;

  const data = await fetchJson(`/api/libraries/${libId}/files?offset=${offset}&limit=25`);
  const total = data.total ?? 0;
  const files = data.files ?? [];
  const page = Math.floor(offset / 25);
  const totalPages = Math.max(Math.ceil(total / 25), 1);

  container.innerHTML = `
    <div class="library-files-header">
      <div class="muted">Showing ${offset + 1}-${Math.min(offset + 25, total)} of ${total}</div>
      <div class="pager">
        <button class="ghost" data-page-prev="${libId}" ${page === 0 ? "disabled" : ""}>Prev</button>
        <span class="muted">Page ${page + 1} / ${totalPages}</span>
        <button class="ghost" data-page-next="${libId}" ${page + 1 >= totalPages ? "disabled" : ""}>Next</button>
      </div>
    </div>
    <table class="table subtable">
      <thead>
        <tr><th>Name</th><th>Status</th><th>Initial</th><th>Final</th><th>New Name</th></tr>
      </thead>
      <tbody>
        ${files
          .map((file) => {
            const name = file.path ? file.path.split(/[/\\]/).pop() : "-";
            const initialSize = formatBytes(file.initial_size ?? file.size ?? 0);
            const initialContainer = normalizeContainerDisplay(file.initial_container ?? "-");
            const initialCodec = file.initial_codec ?? "-";
            const initialAudio = file.initial_audio_codec ?? "-";
            const initialSubs = formatSubtitleList(file.initial_subtitles);
            const initialDuration = formatDurationSeconds(file.initial_duration_sec);
            const initialFrames = formatFrameCount(file.initial_frame_count);
            const finalSize = file.final_size ? formatBytes(file.final_size) : "-";
            const finalContainer = normalizeContainerDisplay(file.final_container ?? "-");
            const finalCodec = file.final_codec ?? "-";
            const finalAudio = file.final_audio_codec ?? "-";
            const finalSubs = formatSubtitleList(file.final_subtitles);
            const finalDuration = formatDurationSeconds(file.final_duration_sec);
            const finalFrames = formatFrameCount(file.final_frame_count);
            const newName = file.new_path ? file.new_path.split(/[/\\]/).pop() : "-";
            const initialText = `${initialSize} · ${initialContainer} · ${initialCodec} · ${initialAudio} · ${initialSubs} · ${initialDuration} · ${initialFrames}`;
            const finalText = `${finalSize} · ${finalContainer} · ${finalCodec} · ${finalAudio} · ${finalSubs} · ${finalDuration} · ${finalFrames}`;
            return `
              <tr>
                <td title="${file.path ?? ""}">${name}</td>
                <td>${file.status}</td>
                <td>${initialText}</td>
                <td>${finalText}</td>
                <td title="${file.new_path ?? ""}">${newName}</td>
              </tr>
            `;
          })
          .join("")}
      </tbody>
    </table>
  `;

  const prevBtn = container.querySelector("button[data-page-prev]");
  const nextBtn = container.querySelector("button[data-page-next]");
  prevBtn?.addEventListener("click", async () => {
    const nextOffset = Math.max(offset - 25, 0);
    libraryFilesPage.set(libId, Math.max(page - 1, 0));
    await renderLibraryFiles(libId, nextOffset);
  });
  nextBtn?.addEventListener("click", async () => {
    const nextOffset = offset + 25;
    libraryFilesPage.set(libId, page + 1);
    await renderLibraryFiles(libId, nextOffset);
  });
}

function renderLibraryModal(nodeNames, trees = []) {
  return `
    <div class="modal-backdrop hidden" id="library-modal-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="library-modal-title">Add Library</h3>
          <button class="ghost" id="library-modal-close">Close</button>
        </div>
        <form class="form" id="library-form">
          <input name="name" placeholder="Library name" required />
          <input name="path" placeholder="Path (e.g. D:\\Media)" required />
          <input name="includeExtensions" placeholder="Include extensions (csv)" />
          <input name="excludeExtensions" placeholder="Exclude extensions (csv)" />
          <input name="nodes" placeholder="Allowed nodes (csv)" list="node-names" />
          <input name="scanIntervalMin" placeholder="Scan interval minutes (default 15)" />
          <label class="field-label">Tree selection</label>
          <select name="treeScope">
            <option value="selected">Selected trees</option>
            <option value="any">Any tree</option>
          </select>
          <div class="field-hint">Choose which trees are eligible for this library.</div>
          <div class="checkbox-group" id="tree-allowlist">
            ${trees
              .map(
                (tree) =>
                  `<label><input type="checkbox" name="allowedTrees" value="${tree.id}" /> ${tree.name} (v${tree.latest_version ?? "-"})</label>`
              )
              .join("")}
          </div>
          <datalist id="node-names">
            ${nodeNames.map((name) => `<option value="${name}"></option>`).join("")}
          </datalist>
          <input type="hidden" name="libraryId" />
          <button class="primary" type="submit">Save Library</button>
        </form>
      </div>
    </div>
  `;
}

function renderNodeSettingsModal() {
  return `
    <div class="modal-backdrop hidden" id="node-settings-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="node-settings-title">Node Limits</h3>
          <button class="ghost" id="node-settings-close">Close</button>
        </div>
        <form class="form" id="node-settings-form">
          <div class="field-hint" id="gpu-targets-hint"></div>
          <label class="field-label" for="healthcheckSlotsCpu">Healthcheck CPU slots</label>
          <input id="healthcheckSlotsCpu" name="healthcheckSlotsCpu" placeholder="e.g. 2" />
          <label class="field-label" for="healthcheckSlotsGpu">Healthcheck GPU slots</label>
          <input id="healthcheckSlotsGpu" name="healthcheckSlotsGpu" placeholder="e.g. 1" />
          <label class="field-label" for="targetHealthcheckCpu">Healthcheck CPU target % (-1 to ignore)</label>
          <input id="targetHealthcheckCpu" name="targetHealthcheckCpu" placeholder="e.g. 60" />
          <label class="field-label" for="healthcheckGpuTargets">Healthcheck GPU targets (comma list)</label>
          <input id="healthcheckGpuTargets" name="healthcheckGpuTargets" placeholder="e.g. 70,60" />
          <label class="field-label" for="transcodeSlotsCpu">Transcode CPU slots</label>
          <input id="transcodeSlotsCpu" name="transcodeSlotsCpu" placeholder="e.g. 1" />
          <label class="field-label" for="transcodeSlotsGpu">Transcode GPU slots</label>
          <input id="transcodeSlotsGpu" name="transcodeSlotsGpu" placeholder="e.g. 2" />
          <label class="field-label" for="targetTranscodeCpu">Transcode CPU target % (-1 to ignore)</label>
          <input id="targetTranscodeCpu" name="targetTranscodeCpu" placeholder="e.g. 75" />
          <label class="field-label" for="transcodeGpuTargets">Transcode GPU targets (comma list)</label>
          <input id="transcodeGpuTargets" name="transcodeGpuTargets" placeholder="e.g. 80,70" />
          <button class="primary" type="submit">Save Limits</button>
        </form>
      </div>
    </div>
  `;
}

function openNodeSettingsModal(node) {
  const backdrop = document.getElementById("node-settings-backdrop");
  if (!backdrop || !node) return;
  const form = document.getElementById("node-settings-form");
  const title = document.getElementById("node-settings-title");
  if (title) title.textContent = `Node Limits: ${node.name}`;

  const settings = node.settings ?? {};
  const gpuCount = Array.isArray(node.hardware?.gpus) ? node.hardware.gpus.length : 0;
  nodeSettingsState = {
    nodeId: node.id,
    name: node.name,
    values: {
      healthcheckSlotsCpu: settings.healthcheckSlotsCpu ?? "",
      healthcheckSlotsGpu: settings.healthcheckSlotsGpu ?? "",
      targetHealthcheckCpu: settings.targetHealthcheckCpu ?? "",
      healthcheckGpuTargets: normalizeGpuTargetList(settings.healthcheckGpuTargets ?? "", gpuCount),
      transcodeSlotsCpu: settings.transcodeSlotsCpu ?? "",
      transcodeSlotsGpu: settings.transcodeSlotsGpu ?? "",
      targetTranscodeCpu: settings.targetTranscodeCpu ?? "",
      transcodeGpuTargets: normalizeGpuTargetList(settings.transcodeGpuTargets ?? "", gpuCount),
    },
    gpuCount,
  };

  if (form) {
    form.healthcheckSlotsCpu.value = nodeSettingsState.values.healthcheckSlotsCpu;
    form.healthcheckSlotsGpu.value = nodeSettingsState.values.healthcheckSlotsGpu;
    form.targetHealthcheckCpu.value = nodeSettingsState.values.targetHealthcheckCpu;
    form.healthcheckGpuTargets.value = nodeSettingsState.values.healthcheckGpuTargets;
    form.transcodeSlotsCpu.value = nodeSettingsState.values.transcodeSlotsCpu;
    form.transcodeSlotsGpu.value = nodeSettingsState.values.transcodeSlotsGpu;
    form.targetTranscodeCpu.value = nodeSettingsState.values.targetTranscodeCpu;
    form.transcodeGpuTargets.value = nodeSettingsState.values.transcodeGpuTargets;
  }

  const hint = document.getElementById("gpu-targets-hint");
  if (hint) {
    hint.textContent = gpuCount
      ? `GPU targets expect ${gpuCount} values. -1 = ignore utilization, 0 = disable GPU.`
      : "No GPUs detected for this node.";
  }

  backdrop.classList.remove("hidden");
  isNodeSettingsModalOpen = true;
}

function closeNodeSettingsModal() {
  const backdrop = document.getElementById("node-settings-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isNodeSettingsModalOpen = false;
}

function bindNodeSettingsModal() {
  const backdrop = document.getElementById("node-settings-backdrop");
  const form = document.getElementById("node-settings-form");
  const closeBtn = document.getElementById("node-settings-close");
  if (!backdrop || !form || !closeBtn) return;

  closeBtn.addEventListener("click", () => closeNodeSettingsModal());
  backdrop.addEventListener("click", (event) => {
    if (event.target === backdrop) closeNodeSettingsModal();
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!nodeSettingsState.nodeId) return;
    const payload = {
      healthcheckSlotsCpu: parseOptionalNumber(form.healthcheckSlotsCpu.value),
      healthcheckSlotsGpu: parseOptionalNumber(form.healthcheckSlotsGpu.value),
      targetHealthcheckCpu: parseOptionalNumber(form.targetHealthcheckCpu.value),
      healthcheckGpuTargets: normalizeGpuTargetList(form.healthcheckGpuTargets.value, nodeSettingsState.gpuCount),
      transcodeSlotsCpu: parseOptionalNumber(form.transcodeSlotsCpu.value),
      transcodeSlotsGpu: parseOptionalNumber(form.transcodeSlotsGpu.value),
      targetTranscodeCpu: parseOptionalNumber(form.targetTranscodeCpu.value),
      transcodeGpuTargets: normalizeGpuTargetList(form.transcodeGpuTargets.value, nodeSettingsState.gpuCount),
    };

    await fetch(`/api/nodes/${nodeSettingsState.nodeId}/settings`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    closeNodeSettingsModal();
    await renderNodes();
  });
}

function parseOptionalNumber(value) {
  if (value === undefined || value === null) return undefined;
  const trimmed = String(value).trim();
  if (!trimmed) return undefined;
  const num = Number(trimmed);
  return Number.isFinite(num) ? num : undefined;
}

function parseOptionalText(value) {
  if (value === undefined || value === null) return undefined;
  const trimmed = String(value).trim();
  return trimmed.length ? trimmed : undefined;
}

function normalizeGpuTargetList(value, gpuCount) {
  if (!gpuCount || gpuCount <= 0) return "";
  const parts = String(value ?? "")
    .split(/[,\s]+/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
    .map((part) => Number(part))
    .map((num) => (Number.isFinite(num) ? num : 0));

  const capped = parts.slice(0, gpuCount);
  while (capped.length < gpuCount) capped.push(0);
  return capped.join(",");
}

async function openLibraryModal(library) {
  const backdrop = document.getElementById("library-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.remove("hidden");
  isLibraryModalOpen = true;

  const title = document.getElementById("library-modal-title");
  title.textContent = library ? "Edit Library" : "Add Library";

  const form = document.getElementById("library-form");
  form.reset();
  form.elements.libraryId.value = library?.id ?? "";
  form.elements.name.value = library?.name ?? "";
  form.elements.path.value = library?.path ?? "";
  form.elements.includeExtensions.value = parseJsonList(library?.include_exts).join(", ");
  form.elements.excludeExtensions.value = parseJsonList(library?.exclude_exts).join(", ");
  form.elements.nodes.value = parseJsonList(library?.nodes_json).join(", ");
  form.elements.scanIntervalMin.value = library?.scan_interval_min ?? "";

  await applyLibraryTreeSelection(library);
}

async function applyLibraryTreeSelection(library) {
  const form = document.getElementById("library-form");
  if (!form) return;
  const scopeField = form.elements.treeScope;
  const allowlist = document.getElementById("tree-allowlist");

  if (!library?.id) {
    if (scopeField) scopeField.value = "selected";
    allowlist?.classList.remove("hidden");
    allowlist
      ?.querySelectorAll("input[name='allowedTrees']")
      .forEach((input) => (input.checked = false));
    return;
  }

  try {
    const data = await fetchJson(`/api/libraries/${library.id}/trees`);
    const treeScope = data?.tree_scope ?? library?.tree_scope ?? "selected";
    if (scopeField) scopeField.value = treeScope;
    allowlist?.classList.toggle("hidden", treeScope === "any");
    const allowedIds = new Set((data?.trees ?? []).map((row) => row.tree_id));
    allowlist
      ?.querySelectorAll("input[name='allowedTrees']")
      .forEach((input) => {
        input.checked = allowedIds.has(input.value);
      });
  } catch {
    if (scopeField) scopeField.value = library?.tree_scope ?? "selected";
    allowlist?.classList.toggle("hidden", scopeField?.value === "any");
  }
}

function closeLibraryModal() {
  const backdrop = document.getElementById("library-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isLibraryModalOpen = false;
}

function renderLogModal() {
  if (document.getElementById("log-modal-backdrop")) {
    return "";
  }
  return `
    <div class="modal-backdrop hidden" id="log-modal-backdrop">
      <div class="modal log-modal">
        <div class="modal-header">
          <h3 id="log-modal-title">Job Logs</h3>
          <button class="ghost" id="log-modal-close">Close</button>
        </div>
        <div class="log-meta" id="log-meta">
          <div class="log-meta-header">
            <button class="ghost log-meta-toggle" id="log-meta-toggle" type="button" aria-expanded="false">
              <span>Details</span>
              <span class="log-meta-caret">▾</span>
            </button>
            <div class="log-meta-summary" id="log-meta-summary"></div>
          </div>
          <div class="log-meta-body" id="log-meta-body"></div>
        </div>
        <div class="log-layout">
          <div class="log-list" id="log-list"></div>
          <div class="log-content" id="log-content">
            <div class="muted">Select a log entry.</div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function parseJobLogEntries(job) {
  const raw = job?.log_text ?? "";
  if (!raw.trim()) {
    return [
      {
        id: 0,
        stage: job?.type ?? "unknown",
        ts: null,
        message: "(no log)",
        order: 0,
      },
    ];
  }

  const lines = raw.split("\n").filter((line) => line.trim().length);
  const entries = [];
  const legacyLines = [];

  lines.forEach((line, index) => {
    try {
      const parsed = JSON.parse(line);
      entries.push({
        id: entries.length,
        stage: parsed.stage ?? job?.type ?? "unknown",
        ts: parsed.ts ?? null,
        message: parsed.message ?? "",
        order: index,
      });
    } catch {
      legacyLines.push(line);
    }
  });

  if (!entries.length) {
    return [
      {
        id: 0,
        stage: job?.type ?? "unknown",
        ts: null,
        message: raw,
        order: 0,
      },
    ];
  }

  const ordered = [...entries].sort((a, b) => a.order - b.order);
  const runs = [];
  const runCounts = new Map();

  const createRun = (label, ts) => {
    const run = {
      id: runs.length,
      stage: label,
      ts: ts ?? null,
      order: runs.length,
      messages: [],
    };
    runs.push(run);
    return run;
  };

  let currentRun = null;

  ordered.forEach((entry) => {
    const message = entry.message ?? "";
    const isRunStart = entry.stage === "system" && message.startsWith("Run started");
    if (isRunStart) {
      const typeMatch = message.match(/type=([a-z0-9_]+)/i);
      const runType = (typeMatch?.[1] ?? "run").toLowerCase();
      const count = (runCounts.get(runType) ?? 0) + 1;
      runCounts.set(runType, count);
      currentRun = createRun(`${runType} run #${count}`, entry.ts);
    }

    if (!currentRun) {
      const count = (runCounts.get("legacy") ?? 0) + 1;
      runCounts.set("legacy", count);
      currentRun = createRun(`legacy run #${count}`, entry.ts);
    }

    currentRun.messages.push(message);
  });

  if (legacyLines.length) {
    if (!currentRun) {
      const count = (runCounts.get("legacy") ?? 0) + 1;
      runCounts.set("legacy", count);
      currentRun = createRun(`legacy run #${count}`, null);
    }
    currentRun.messages.push(...legacyLines);
  }

  return runs.map((run, index) => ({
    id: index,
    stage: run.stage,
    ts: run.ts,
    message: run.messages.join("\n"),
    order: run.order,
  }));
}

function formatLogTimestamp(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
}

function openLogModal(job) {
  const backdrop = document.getElementById("log-modal-backdrop");
  if (!backdrop) return;
  if (backdrop.parentElement !== document.body) {
    document.body.appendChild(backdrop);
  }

  currentLogJobId = job?.id ?? null;
  lastLogSnapshot = job?.log_text ?? "";
  lastLogStatus = job?.status ?? "";
  updateLogModal(job);

  backdrop.classList.remove("hidden");
  isLogModalOpen = true;
  const metaToggle = document.getElementById("log-meta-toggle");
  const metaBody = document.getElementById("log-meta-body");
  if (metaToggle && metaBody) {
    metaBody.classList.add("collapsed");
    metaToggle.setAttribute("aria-expanded", "false");
  }
  startLogPolling();
}

function closeLogModal() {
  const backdrop = document.getElementById("log-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isLogModalOpen = false;
  currentLogJobId = null;
  currentLogEntries = [];
  stopLogPolling();
}

function updateLogModal(job) {
  if (!job) return;
  currentLogEntries = parseJobLogEntries(job)
    .map((entry, index) => ({ ...entry, id: index }))
    .sort((a, b) => {
      const aTime = a.ts ? new Date(a.ts).getTime() : -Infinity;
      const bTime = b.ts ? new Date(b.ts).getTime() : -Infinity;
      if (aTime === bTime) return b.order - a.order;
      return bTime - aTime;
    });

  const title = document.getElementById("log-modal-title");
  if (title) title.textContent = `Job ${job.id} logs`;

  const meta = document.getElementById("log-meta");
  if (meta) {
    const fileName = getFileName(job.file_path ?? job.new_path ?? "") || "-";
    const initialSize = formatBytes(job.initial_size ?? job.file_size ?? 0);
    const initialContainer = normalizeContainerDisplay(job.initial_container ?? "-");
    const initialCodec = job.initial_codec ?? "-";
    const initialAudio = job.initial_audio_codec ?? "-";
    const initialSubs = formatSubtitleList(job.initial_subtitles);
    const initialDuration = formatDurationSeconds(job.initial_duration_sec);
    const initialFrames = formatFrameCount(job.initial_frame_count);
    const finalSize = job.final_size ? formatBytes(job.final_size) : "-";
    const finalContainer = normalizeContainerDisplay(job.final_container ?? "-");
    const finalCodec = job.final_codec ?? "-";
    const finalAudio = job.final_audio_codec ?? "-";
    const finalSubs = formatSubtitleList(job.final_subtitles);
    const finalDuration = formatDurationSeconds(job.final_duration_sec);
    const finalFrames = formatFrameCount(job.final_frame_count);
    const newName = getFileName(job.new_path ?? "") || "-";

    const summary = document.getElementById("log-meta-summary");
    if (summary) {
      summary.innerHTML = `
        <div><span class="muted">Job ID:</span> ${job.id}</div>
        <div><span class="muted">File:</span> ${fileName}</div>
      `;
    }

    const body = document.getElementById("log-meta-body");
    if (body) {
      body.innerHTML = `
        <div class="log-meta-grid">
          <div>
            <div class="muted">Job ID</div>
            <div>${job.id}</div>
          </div>
          <div>
            <div class="muted">File</div>
            <div title="${job.file_path ?? ""}">${fileName}</div>
          </div>
          <div>
            <div class="muted">Type</div>
            <div>${job.type ?? "-"}</div>
          </div>
          <div>
            <div class="muted">Status</div>
            <div>${normalizeJobStatus(job.status)}</div>
          </div>
          <div>
            <div class="muted">Initial</div>
            <div>${initialSize} · ${initialContainer} · ${initialCodec} · ${initialAudio} · ${initialSubs} · ${initialDuration} · ${initialFrames}</div>
          </div>
          <div>
            <div class="muted">Final</div>
            <div>${finalSize} · ${finalContainer} · ${finalCodec} · ${finalAudio} · ${finalSubs} · ${finalDuration} · ${finalFrames}</div>
          </div>
          <div>
            <div class="muted">New Name</div>
            <div title="${job.new_path ?? ""}">${newName}</div>
          </div>
        </div>
      `;
    }
  }

  const list = document.getElementById("log-list");
  const content = document.getElementById("log-content");
  const activeIndex = Number(
    list?.querySelector(".log-item.active")?.dataset?.logIndex ?? 0
  );

  if (list) {
    list.innerHTML = currentLogEntries
      .map((entry, index) => {
        const tsText = formatLogTimestamp(entry.ts);
        const label = entry.stage ?? "unknown";
        return `
          <button class="log-item ${index === activeIndex ? "active" : ""}" data-log-index="${index}">
            <div class="log-item-title">${label}</div>
            <div class="muted log-item-sub">${tsText || ""}</div>
          </button>
        `;
      })
      .join("");
  }

  if (content) {
    const entry = currentLogEntries[activeIndex] ?? currentLogEntries[0];
    content.innerHTML = renderLogContent(entry?.message ?? "(no log)");
  }
}

function startLogPolling() {
  if (logPollTimer) return;
  logPollTimer = setInterval(async () => {
    if (!isLogModalOpen || !currentLogJobId) return;
    try {
      const job = await fetchJson(`/api/jobs/${currentLogJobId}`);
      const raw = job?.log_text ?? "";
      const status = job?.status ?? "";
      if (raw !== lastLogSnapshot || status !== lastLogStatus) {
        lastLogSnapshot = raw;
        lastLogStatus = status;
        updateLogModal(job);
      }
    } catch {
      // ignore polling errors
    }
  }, 1000);
}

function stopLogPolling() {
  if (!logPollTimer) return;
  clearInterval(logPollTimer);
  logPollTimer = null;
}

function bindLogModal() {
  const backdrop = document.getElementById("log-modal-backdrop");
  const closeBtn = document.getElementById("log-modal-close");
  const list = document.getElementById("log-list");
  const content = document.getElementById("log-content");
  const metaToggle = document.getElementById("log-meta-toggle");
  const metaBody = document.getElementById("log-meta-body");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeLogModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeLogModal();
  });

  metaToggle?.addEventListener("click", () => {
    if (!metaBody) return;
    const isCollapsed = metaBody.classList.toggle("collapsed");
    metaToggle.setAttribute("aria-expanded", String(!isCollapsed));
  });

  list?.addEventListener("click", (event) => {
    const target = event.target.closest("button[data-log-index]");
    if (!target) return;
    const index = Number(target.dataset.logIndex);
    const entry = currentLogEntries[index];
    if (!entry || !content) return;

    list.querySelectorAll(".log-item").forEach((item) => {
      item.classList.toggle("active", item === target);
    });

    content.innerHTML = renderLogContent(entry.message ?? "(no log)");
  });
}

function renderLogContent(message) {
  const text = String(message ?? "");
  const segments = splitLogSegments(text);
  const rendered = [];
  let lineIndex = 0;

  const pushLine = (line) => {
    if (!String(line ?? "").trim()) {
      rendered.push(`<div class="log-line empty"></div>`);
    } else {
      rendered.push(
        `<div class="log-line ${lineIndex % 2 ? "odd" : "even"}">${escapeHtml(line)}</div>`
      );
    }
    lineIndex += 1;
  };

  const pushJson = (json, prefix = "") => {
    rendered.push(
      `
        <div class="log-line ${lineIndex % 2 ? "odd" : "even"}">
          ${prefix ? `<div class=\"log-json-prefix\">${escapeHtml(prefix)}</div>` : ""}
          <details class="log-json">
            <summary>JSON</summary>
            <pre>${escapeHtml(JSON.stringify(json, null, 2))}</pre>
          </details>
        </div>
      `
    );
    lineIndex += 1;
  };

  segments.forEach((segment) => {
    if (segment.type === "json") {
      pushJson(segment.json, segment.prefix);
      return;
    }
    const lines = String(segment.value ?? "").split(/\r?\n/);
    lines.forEach((line) => pushLine(line));
  });

  return `<div class="log-content-lines">${rendered.join("")}</div>`;
}

function safeParseJson(value) {
  try {
    const parsed = JSON.parse(value);
    if (parsed && typeof parsed === "object") return parsed;
  } catch {
    // ignore
  }
  return null;
}

function splitLogSegments(text) {
  const segments = [];
  const value = String(text ?? "");
  let cursor = 0;
  let scanIndex = 0;

  while (scanIndex < value.length) {
    const startIndex = findJsonStart(value, scanIndex);
    if (startIndex < 0) {
      segments.push({ type: "text", value: value.slice(cursor) });
      break;
    }

    const block = scanJsonBlockText(value, startIndex);
    if (!block) {
      scanIndex = startIndex + 1;
      continue;
    }

    const lineStart = value.lastIndexOf("\n", startIndex - 1) + 1;
    const prefixText = value.slice(lineStart, startIndex);
    const prefix = prefixText.trim();
    const prefixHasText = prefix.length > 0;
    const beforeEnd = prefixHasText ? lineStart : startIndex;

    if (beforeEnd > cursor) {
      segments.push({ type: "text", value: value.slice(cursor, beforeEnd) });
    }

    segments.push({ type: "json", json: block.json, prefix: prefixHasText ? prefix : "" });
    cursor = block.endIndex + 1;
    scanIndex = cursor;
  }

  return segments;
}

function findJsonStart(text, fromIndex) {
  const isTagPrefix = (idx) => {
    if (text[idx] !== "[") return false;
    const lineEnd = text.indexOf("\n", idx);
    const end = lineEnd === -1 ? text.length : lineEnd;
    const closeIdx = text.indexOf("]", idx + 1);
    if (closeIdx === -1 || closeIdx >= end) return false;
    const label = text.slice(idx + 1, closeIdx).trim();
    if (!label || /[^a-z0-9_.:-]/i.test(label)) return false;
    const next = text[closeIdx + 1];
    return next === " " || next === "\t";
  };

  for (let i = fromIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === "[") {
      if (isTagPrefix(i)) {
        const skip = text.indexOf("]", i + 1);
        i = skip > -1 ? skip : i;
        continue;
      }
      return i;
    }
    if (ch === "{") return i;
  }
  return -1;
}

function scanJsonBlockText(text, startIndex) {
  let depth = 0;
  let inString = false;
  let escape = false;
  let started = false;
  for (let i = startIndex; i < text.length; i += 1) {
    const ch = text[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (ch === "\\") {
      escape = true;
      continue;
    }
    if (ch === '"') {
      inString = !inString;
      continue;
    }
    if (inString) continue;

    if (ch === "{" || ch === "[") {
      depth += 1;
      started = true;
    } else if (ch === "}" || ch === "]") {
      depth -= 1;
    }

    if (started && depth === 0) {
      const raw = text.slice(startIndex, i + 1);
      const parsed = safeParseJson(raw.trim());
      if (parsed) {
        return { json: parsed, endIndex: i };
      }
      return null;
    }
  }
  return null;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function bindLibraryModal(libs) {
  const backdrop = document.getElementById("library-modal-backdrop");
  const closeBtn = document.getElementById("library-modal-close");
  const form = document.getElementById("library-form");
  const allowlist = document.getElementById("tree-allowlist");
  const scopeField = form?.elements?.treeScope;

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeLibraryModal();
  });
  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeLibraryModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());
    const includeExtensions = data.includeExtensions
      ? data.includeExtensions.split(/[,\s]+/).filter(Boolean)
      : undefined;
    const excludeExtensions = data.excludeExtensions
      ? data.excludeExtensions.split(/[,\s]+/).filter(Boolean)
      : undefined;
    const nodes = data.nodes ? data.nodes.split(/[,\s]+/).filter(Boolean) : undefined;
    const scanIntervalMin = data.scanIntervalMin ? Number(data.scanIntervalMin) : undefined;
    const treeScope = data.treeScope ?? "selected";
    const allowedTrees = treeScope === "selected" ? formData.getAll("allowedTrees") : [];

    const payload = {
      name: data.name,
      path: data.path,
      includeExtensions,
      excludeExtensions,
      nodes,
      scanIntervalMin,
      treeScope,
      allowedTrees,
    };

    if (data.libraryId) {
      await fetch(`/api/libraries/${data.libraryId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    } else {
      await fetch("/api/libraries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
    }

    closeLibraryModal();
    await renderLibraries();
  });

  scopeField?.addEventListener("change", () => {
    const isSelected = scopeField.value !== "any";
    allowlist?.classList.toggle("hidden", !isSelected);
  });

  const expanded = Array.from(expandedLibraries);
  for (const libId of expanded) {
    const page = libraryFilesPage.get(libId) ?? 0;
    renderLibraryFiles(libId, page * 25);
  }
}

function renderTreeModal() {
  return `
    <div class="modal-backdrop hidden" id="tree-modal-backdrop">
      <div class="modal tree-modal">
        <div class="modal-header">
          <h3 id="tree-modal-title">Create Tree</h3>
          <button class="ghost" id="tree-modal-close">Close</button>
        </div>
        <form class="form" id="tree-form">
          <input name="name" placeholder="Tree name" required />
          <textarea name="description" placeholder="Description"></textarea>
          <label class="field-label">Processing support</label>
          <select name="requiredProcessing">
            <option value="any">Any (CPU/GPU)</option>
            <option value="cpu">CPU only</option>
            <option value="gpu">GPU only</option>
          </select>
          <label class="field-label">Supported accelerators (any)</label>
          <div class="checkbox-group">
            <label><input type="checkbox" name="requiredAccelerators" value="cpu" /> CPU</label>
            <label><input type="checkbox" name="requiredAccelerators" value="nvidia" /> NVIDIA</label>
            <label><input type="checkbox" name="requiredAccelerators" value="intel" /> Intel</label>
            <label><input type="checkbox" name="requiredAccelerators" value="amd" /> AMD</label>
          </div>
          <div class="field-hint">If multiple are checked, any matching accelerator is allowed.</div>
          <label class="field-label">Node tags (all required)</label>
          <input name="requiredTagsAll" placeholder="e.g. h265, hdr" />
          <label class="field-label">Node tags (any required)</label>
          <input name="requiredTagsAny" placeholder="e.g. nvidia, intel" />
          <label class="field-label">Node tags (must be absent)</label>
          <input name="requiredTagsNone" placeholder="e.g. lowpower" />
          <div class="field-hint">Tags are CSV values set on the node config.</div>
          <input type="hidden" name="treeId" />
          <button class="primary" type="submit">Save</button>
        </form>
      </div>
    </div>
  `;
}

function renderTreeAssignModal(libraries) {
  return `
    <div class="modal-backdrop hidden" id="tree-assign-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="tree-assign-title">Assign Tree</h3>
          <button class="ghost" id="tree-assign-close">Close</button>
        </div>
        <form class="form" id="tree-assign-form">
          <select name="libraryId" required>
            <option value="">Select library...</option>
            ${libraries
              .map((lib) => `<option value="${lib.id}">${lib.name}</option>`)
              .join("")}
          </select>
          <input name="treeId" type="hidden" />
          <input name="treeVersion" placeholder="Tree version (e.g. 1)" required />
          <button class="primary" type="submit">Assign</button>
        </form>
      </div>
    </div>
  `;
}

function renderTreeSettingsModal() {
  return `
    <div class="modal-backdrop hidden" id="tree-settings-backdrop">
      <div class="modal">
        <div class="modal-header">
          <h3 id="tree-settings-title">Node Settings</h3>
          <button class="ghost" id="tree-settings-close">Close</button>
        </div>
        <div class="tree-settings-info" id="tree-settings-info"></div>
        <div class="tree-settings-error hidden" id="tree-settings-error"></div>
        <form class="form" id="tree-settings-form"></form>
      </div>
    </div>
  `;
}

function openTreeSettingsModal(node) {
  const backdrop = document.getElementById("tree-settings-backdrop");
  const form = document.getElementById("tree-settings-form");
  const title = document.getElementById("tree-settings-title");
  const info = document.getElementById("tree-settings-info");
  const error = document.getElementById("tree-settings-error");
  if (!backdrop || !form || !title) return;

  const elementType = node?.data?.elementType ?? node?.elementType ?? "unknown";
  const def = getElementDef(elementType) ?? {
    type: elementType,
    label: node?.data?.label ?? elementType,
    source: "built-in",
    fields: [],
  };
  const nodeId = node?.id ?? node?.data?.__nodeId ?? node?.nodeId ?? "";
  title.textContent = `Settings: ${node?.data?.label ?? def.label ?? elementType}`;
  form.dataset.nodeId = nodeId;

  const config = node?.data?.config ?? {};
  form.innerHTML = buildSettingsFields(def, config);

  if (info) {
    const sourceLabel = def?.source === "plugin" ? `Plugin (${def?.plugin ?? "custom"})` : "Built-in";
    info.innerHTML = `
      <div><strong>Name</strong> ${def?.label ?? elementType}</div>
      <div><strong>Type</strong> ${elementType}</div>
      <div><strong>Source</strong> ${sourceLabel}</div>
      <div><strong>Description</strong> ${def?.description ?? "No description."}</div>
      <div><strong>Usage</strong> ${def?.usage ?? "No usage notes."}</div>
    `;
  }

  if (error) {
    error.textContent = "";
    error.classList.add("hidden");
  }

  backdrop.classList.remove("hidden");
  isTreeModalOpen = true;
}

function ensureTreeNodeMenu() {
  let menu = document.getElementById("tree-node-menu");
  if (menu) return menu;
  menu = document.createElement("div");
  menu.id = "tree-node-menu";
  menu.className = "tree-context-menu hidden";
  menu.innerHTML = `
    <button class="ghost" data-action="config">Configure</button>
    <button class="danger" data-action="delete">Delete</button>
  `;
  document.body.appendChild(menu);

  menu.addEventListener("click", (event) => {
    const action = event.target.closest("button")?.dataset.action;
    if (!action || !treeNodeMenuState.node) return;
    if (action === "config") {
      openTreeSettingsModal(treeNodeMenuState.node);
      closeTreeNodeMenu();
      return;
    }
    if (action === "delete") {
      const nodeId = treeNodeMenuState.node?.id ?? treeNodeMenuState.node?.data?.__nodeId;
      if (nodeId && typeof treeDesignerState.deleteNode === "function") {
        treeDesignerState.deleteNode(nodeId);
      }
      closeTreeNodeMenu();
    }
  });

  document.addEventListener("click", (event) => {
    if (!menu.classList.contains("hidden") && !menu.contains(event.target)) {
      closeTreeNodeMenu();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeTreeNodeMenu();
  });

  return menu;
}

function openTreeNodeMenu(position, node) {
  const menu = ensureTreeNodeMenu();
  const x = Math.max(8, position.x ?? 0);
  const y = Math.max(8, position.y ?? 0);
  menu.style.left = `${x}px`;
  menu.style.top = `${y}px`;
  menu.classList.remove("hidden");
  treeNodeMenuState.node = node;
}

function closeTreeNodeMenu() {
  const menu = document.getElementById("tree-node-menu");
  if (!menu) return;
  menu.classList.add("hidden");
  treeNodeMenuState.node = null;
}

function closeTreeSettingsModal() {
  const backdrop = document.getElementById("tree-settings-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isTreeModalOpen = false;
}

function bindTreeSettingsModal() {
  const backdrop = document.getElementById("tree-settings-backdrop");
  const closeBtn = document.getElementById("tree-settings-close");
  const form = document.getElementById("tree-settings-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeTreeSettingsModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeTreeSettingsModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const nodeId = form.dataset.nodeId;
    if (!nodeId || typeof treeDesignerState.updateNodeConfig !== "function") return;
    const data = Object.fromEntries(new FormData(form).entries());
    const elementType = data.elementType;
    const def = getElementDef(elementType);
    const { config, errors } = parseSettingsConfig(def, data);
    const error = document.getElementById("tree-settings-error");
    if (errors.length) {
      if (error) {
        error.innerHTML = errors.map((item) => `<div>${item}</div>`).join("");
        error.classList.remove("hidden");
      }
      return;
    }
    if (error) {
      error.textContent = "";
      error.classList.add("hidden");
    }
    treeDesignerState.updateNodeConfig(nodeId, config, { autoSave: true });
    closeTreeSettingsModal();
  });
}

function buildSettingsFields(def, config) {
  const fields = [];
  const elementType = def?.type ?? "unknown";
  fields.push(`<input type="hidden" name="elementType" value="${elementType}" />`);

  const fieldDefs = def?.fields ?? [];
  fieldDefs.forEach((field, index) => {
    const label = field.label ?? field.key;
    const name = field.name ?? field.key;
    const placeholder = field.placeholder ?? "";
    const type = field.type ?? "text";
    const suggestions = Array.isArray(field.suggestions) ? field.suggestions : [];
    const fieldId = `field-${elementType}-${name.replace(/[^a-z0-9_-]/gi, "-")}-${index}`;
    const listId = suggestions.length
      ? `suggest-${elementType}-${name.replace(/[^a-z0-9_-]/gi, "-")}`
      : null;
    const current = getConfigValue(config, field.path ?? field.key, field.default);
    const value = type === "json"
      ? JSON.stringify(current ?? field.default ?? {}, null, 2)
      : type === "textarea"
        ? Array.isArray(current) && field.format === "lines"
          ? current.join("\n")
          : current ?? ""
        : current ?? "";
    fields.push(`<label class="muted" for="${fieldId}">${label}</label>`);
    if (field.description) {
      fields.push(`<div class="muted" style="font-size:12px;">${field.description}</div>`);
    }
    if (type === "checkbox" || type === "boolean") {
      const checked = Boolean(current ?? field.default ?? false);
      fields.push(
        `<input id="${fieldId}" name="${name}" type="checkbox"${checked ? " checked" : ""} />`
      );
    } else if (type === "textarea" || type === "json") {
      fields.push(
        `<textarea id="${fieldId}" name="${name}" placeholder="${placeholder}" rows="4">${value ?? ""}</textarea>`
      );
    } else {
      const inputType = type === "number" ? "number" : "text";
      fields.push(
        `<input id="${fieldId}" name="${name}" type="${inputType}" value="${value ?? ""}" placeholder="${placeholder}"${listId ? ` list="${listId}"` : ""} />`
      );
      if (listId) {
        fields.push(
          `<datalist id="${listId}">` +
            suggestions.map((item) => `<option value="${item}"></option>`).join("") +
          `</datalist>`
        );
      }
    }
  });

  if (!fieldDefs.length) {
    fields.push(`<div class="muted">No settings for this element.</div>`);
  }

  fields.push(`<button class="primary" type="submit">Save settings</button>`);
  return fields.join("");
}

function parseSettingsConfig(def, data) {
  const config = {};
  const errors = [];
  const fieldDefs = def?.fields ?? [];

  fieldDefs.forEach((field) => {
    const key = field.key;
    const name = field.name ?? key;
    const raw = data[name] ?? data[key];
    const label = field.label ?? key;
    const type = field.type ?? "text";
    const path = field.path ?? key;

    if (field.regex && raw) {
      const re = new RegExp(field.regex);
      if (!re.test(raw)) {
        errors.push(`${label}: invalid format.`);
        return;
      }
    }

    if (raw === undefined || raw === "") {
      if (type === "checkbox" || type === "boolean") {
        const fallback = field.default !== undefined ? field.default : false;
        setDeepValue(config, path, Boolean(fallback));
      } else if (field.default !== undefined) {
        setDeepValue(config, path, field.default);
      }
      return;
    }

    if (type === "number") {
      const num = Number(raw);
      if (Number.isNaN(num)) {
        errors.push(`${label}: must be a number.`);
        return;
      }
      setDeepValue(config, path, num);
      return;
    }

    if (type === "checkbox" || type === "boolean") {
      setDeepValue(config, path, true);
      return;
    }

    if (type === "json") {
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          errors.push(`${label}: must be a JSON object.`);
          return;
        }
        setDeepValue(config, path, parsed);
      } catch {
        errors.push(`${label}: invalid JSON.`);
      }
      return;
    }

    if (type === "textarea") {
      if (field.format === "lines") {
        setDeepValue(
          config,
          path,
          String(raw)
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter(Boolean)
        );
      } else {
        setDeepValue(config, path, String(raw));
      }
      return;
    }

    if (field.format === "csv") {
      setDeepValue(
        config,
        path,
        String(raw)
          .split(/[\s,]+/)
          .map((value) => value.trim())
          .filter(Boolean)
      );
      return;
    }

    setDeepValue(config, path, String(raw));
  });

  return { config, errors };
}

function getConfigValue(config, path, fallback) {
  if (!config) return fallback;
  if (!path) return fallback;
  if (Object.prototype.hasOwnProperty.call(config, path)) {
    return config[path] ?? fallback;
  }
  const segments = path.split(".");
  let current = config;
  for (const segment of segments) {
    if (current && typeof current === "object" && segment in current) {
      current = current[segment];
    } else {
      return fallback;
    }
  }
  return current ?? fallback;
}

function setDeepValue(target, path, value) {
  if (!path) return;
  const segments = path.split(".");
  let current = target;
  segments.forEach((segment, index) => {
    if (index === segments.length - 1) {
      current[segment] = value;
      return;
    }
    if (!current[segment] || typeof current[segment] !== "object") {
      current[segment] = {};
    }
    current = current[segment];
  });
}

function getElementDef(elementType) {
  return TREE_ELEMENT_DEFS.find((item) => item.type === elementType);
}

function openTreeModal(tree) {
  const backdrop = document.getElementById("tree-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.remove("hidden");
  isTreeModalOpen = true;
  const title = document.getElementById("tree-modal-title");
  const form = document.getElementById("tree-form");
  if (!form) return;

  title.textContent = tree ? `Edit Tree` : "Create Tree";
  form.reset();
  form.elements.treeId.value = tree?.id ?? "";
  form.elements.name.value = tree?.name ?? "";
  form.elements.description.value = tree?.description ?? "";
  form.elements.requiredProcessing.value = tree?.required_processing ?? "any";
  const requiredAccels = parseJsonList(tree?.required_accelerators);
  Array.from(form.querySelectorAll("input[name='requiredAccelerators']")).forEach((input) => {
    input.checked = requiredAccels.includes(input.value);
  });
  form.elements.requiredTagsAll.value = parseJsonList(tree?.required_tags_all).join(", ");
  form.elements.requiredTagsAny.value = parseJsonList(tree?.required_tags_any).join(", ");
  form.elements.requiredTagsNone.value = parseJsonList(tree?.required_tags_none).join(", ");
}

function closeTreeModal() {
  const backdrop = document.getElementById("tree-modal-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isTreeModalOpen = false;
}

function bindTreeModal() {
  const backdrop = document.getElementById("tree-modal-backdrop");
  const closeBtn = document.getElementById("tree-modal-close");
  const form = document.getElementById("tree-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeTreeModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeTreeModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const data = Object.fromEntries(formData.entries());
    const requiredAccelerators = formData.getAll("requiredAccelerators");
    const requiredProcessing = data.requiredProcessing ?? "any";
    const requiredTagsAll = data.requiredTagsAll
      ? data.requiredTagsAll.split(/[\,\s]+/).filter(Boolean)
      : [];
    const requiredTagsAny = data.requiredTagsAny
      ? data.requiredTagsAny.split(/[\,\s]+/).filter(Boolean)
      : [];
    const requiredTagsNone = data.requiredTagsNone
      ? data.requiredTagsNone.split(/[\,\s]+/).filter(Boolean)
      : [];
    if (data.treeId) {
      await fetch(`/api/trees/${data.treeId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: data.name,
          description: data.description,
          requiredAccelerators,
          requiredProcessing,
          requiredTagsAll,
          requiredTagsAny,
          requiredTagsNone,
        }),
      });
    } else {
      await fetch("/api/trees", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: data.name,
          description: data.description,
          requiredAccelerators,
          requiredProcessing,
          requiredTagsAll,
          requiredTagsAny,
          requiredTagsNone,
        }),
      });
    }

    closeTreeModal();
    await renderTrees();
  });
}

function openTreeAssignModal(tree, libraries) {
  const backdrop = document.getElementById("tree-assign-backdrop");
  if (!backdrop) return;
  backdrop.classList.remove("hidden");
  isTreeAssignModalOpen = true;
  const title = document.getElementById("tree-assign-title");
  const form = document.getElementById("tree-assign-form");
  if (!form) return;

  title.textContent = `Assign Tree: ${tree?.name ?? ""}`;
  form.reset();
  form.elements.treeId.value = tree?.id ?? "";
  form.elements.treeVersion.value = tree?.latestVersion ?? tree?.latest_version ?? "";
}

function closeTreeAssignModal() {
  const backdrop = document.getElementById("tree-assign-backdrop");
  if (!backdrop) return;
  backdrop.classList.add("hidden");
  isTreeAssignModalOpen = false;
}

function bindTreeAssignModal(libraries) {
  const backdrop = document.getElementById("tree-assign-backdrop");
  const closeBtn = document.getElementById("tree-assign-close");
  const form = document.getElementById("tree-assign-form");

  closeBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    closeTreeAssignModal();
  });

  backdrop?.addEventListener("click", (event) => {
    if (event.target === backdrop) closeTreeAssignModal();
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    const treeVersion = Number(data.treeVersion);
    if (!treeVersion || Number.isNaN(treeVersion)) {
      alert("Tree version must be a number.");
      return;
    }

    await fetch(`/api/libraries/${data.libraryId}/tree`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ treeId: data.treeId, treeVersion }),
    });

    closeTreeAssignModal();
    await renderTrees();
  });
}

function mountTreeDesigner(tree) {
  const container = document.getElementById("tree-designer");
  if (!container || !window.React || !window.ReactDOM || !window.ReactFlow) return;

  isTreeDesignerActive = true;
  treeDesignerState.treeId = tree?.id ?? null;

  const React = window.React;
  const ReactDOM = window.ReactDOM;
  const ReactFlow = window.ReactFlow.ReactFlow;
  const Background = window.ReactFlow.Background;
  const Controls = window.ReactFlow.Controls;
  const MiniMap = window.ReactFlow.MiniMap;
  const addEdge = window.ReactFlow.addEdge;
  const useNodesState = window.ReactFlow.useNodesState;
  const useEdgesState = window.ReactFlow.useEdgesState;
  const useReactFlow = window.ReactFlow.useReactFlow;
  const Handle = window.ReactFlow.Handle;
  const Position = window.ReactFlow.Position;
  const ReactFlowProvider = window.ReactFlow.ReactFlowProvider;

  const initialGraph = tree?.graph ? JSON.parse(tree.graph) : null;
  const initialNodes = (initialGraph?.nodes ?? []).map((node, index) =>
    normalizeElementNode(
      {
        ...node,
        position:
          node?.position && typeof node.position.x === "number" && typeof node.position.y === "number"
            ? node.position
            : { x: 80 + index * 40, y: 80 + index * 40 },
      },
      TREE_ELEMENT_DEFS
    )
  );
  const initialEdges = initialGraph?.edges ?? [];

  const TreeNode = ({ data, id }) => {
    const outputs = data?.outputs ?? [];
    const elementType = data?.elementType;
    return React.createElement(
      "div",
      {
        className: "tree-node",
        "data-node-id": id,
        "data-element-type": elementType ?? "custom",
        "data-label": data?.label ?? "Node",
        onDoubleClick: () => openTreeSettingsModal({ id, data }),
        onMouseDown: (event) => {
          if (event.detail === 2) openTreeSettingsModal({ id, data });
        },
        onContextMenu: (event) => {
          event.preventDefault();
          openTreeNodeMenu({ x: event.clientX, y: event.clientY }, { id, data });
        },
      },
      React.createElement("div", { className: "tree-node-title" }, data?.label ?? "Node"),
      React.createElement(
        "div",
        { className: "tree-node-sub" },
        data?.elementType ?? "custom"
      ),
      elementType === "input"
        ? null
        : React.createElement(Handle, { type: "target", position: Position.Top, id: "in" }),
      outputs.map((output, index) =>
        React.createElement(Handle, {
          key: output.id,
          type: "source",
          position: Position.Bottom,
          id: output.id,
          style: { left: 12 + index * 18 },
        })
      ),
      React.createElement(Handle, {
        type: "source",
        position: Position.Right,
        id: "default",
        className: "tree-node-default-handle",
      })
    );
  };

  const Designer = () => {
    const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
    const [dirty, setDirty] = React.useState(false);
    const { screenToFlowPosition } = useReactFlow();
    const lastClickRef = React.useRef({ nodeId: null, time: 0 });
    const lastPointerRef = React.useRef({ nodeId: null, time: 0 });

    React.useEffect(() => {
      setNodes(initialNodes);
      setEdges(initialEdges);
      setDirty(false);
    }, [tree?.id]);

    React.useEffect(() => {
      treeDesignerState.getNode = (nodeId) => nodes.find((node) => node.id === nodeId);
    }, [nodes]);

    React.useEffect(() => {
      const pointerHandler = (event) => {
        const target = event.target.closest(".tree-node");
        if (!target) return;
        const nodeId = target.dataset.nodeId;
        if (!nodeId) return;
        const now = Date.now();
        const last = lastPointerRef.current;
        if (last.nodeId === nodeId && now - last.time < 350) {
          const node =
            treeDesignerState.getNode?.(nodeId) ?? {
              id: nodeId,
              data: {
                elementType: target.dataset.elementType ?? "custom",
                label: target.dataset.label ?? "Node",
              },
            };
          openTreeSettingsModal(node);
          lastPointerRef.current = { nodeId: null, time: 0 };
          return;
        }
        lastPointerRef.current = { nodeId, time: now };
      };
      document.addEventListener("pointerdown", pointerHandler, true);
      return () => {
        document.removeEventListener("pointerdown", pointerHandler, true);
      };
    }, [nodes]);

    const onNodesChangeWithDirty = React.useCallback(
      (changes) => {
        onNodesChange(changes);
        setDirty(true);
      },
      [onNodesChange]
    );

    const onEdgesChangeWithDirty = React.useCallback(
      (changes) => {
        onEdgesChange(changes);
        setDirty(true);
      },
      [onEdgesChange]
    );

    const onConnect = React.useCallback(
      (params) => {
        setEdges((eds) => addEdge(params, eds));
        setDirty(true);
      },
      []
    );

    const addNode = (elementType, position) => {
      const def = TREE_ELEMENT_DEFS.find((item) => item.type === elementType);
      if (!def) return;
      if (elementType === "input" && nodes.some((node) => node.data?.elementType === "input")) {
        alert("Only one input element is allowed in a tree.");
        return;
      }
      const id = `${elementType}-${Date.now()}`;
      setNodes((nds) => [
        ...nds,
        {
          id,
          type: "treeNode",
          position: position ?? { x: 50 + nds.length * 40, y: 50 + nds.length * 40 },
          data: {
            label: def.label,
            elementType: def.type,
            outputs: def.outputs,
            context: {},
            __nodeId: id,
          },
        },
      ]);
      setDirty(true);
    };

    const onDrop = React.useCallback(
      (event) => {
        event.preventDefault();
        const elementType = event.dataTransfer.getData("application/reactflow");
        if (!elementType) return;
        const position = screenToFlowPosition({
          x: event.clientX,
          y: event.clientY,
        });
        addNode(elementType, position);
      },
      [screenToFlowPosition]
    );

    const onDragOver = React.useCallback((event) => {
      event.preventDefault();
      event.dataTransfer.dropEffect = "move";
    }, []);

    const onNodeDoubleClick = React.useCallback((event, node) => {
      openTreeSettingsModal(node);
    }, []);

    const onNodeClick = React.useCallback((event, node) => {
      const now = Date.now();
      const last = lastClickRef.current;
      if (last.nodeId === node.id && now - last.time < 350) {
        openTreeSettingsModal(node);
        lastClickRef.current = { nodeId: null, time: 0 };
        return;
      }
      lastClickRef.current = { nodeId: node.id, time: now };
    }, []);

    const onNodeContextMenu = React.useCallback((event, node) => {
      event.preventDefault();
      openTreeNodeMenu({ x: event.clientX, y: event.clientY }, node);
    }, []);

    const saveGraph = React.useCallback(
      async (graphNodes, graphEdges) => {
        if (!tree?.id) return;
        const graph = { version: 1, nodes: graphNodes, edges: graphEdges };
        const res = await fetch(`/api/trees/${tree.id}/versions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ graph }),
        });
        if (!res.ok) {
          const message = await res.text();
          alert(`Save failed: ${message || res.status}`);
          return false;
        }
        setDirty(false);
        pendingTreeId = tree.id;
        await renderTrees();
        return true;
      },
      [tree?.id]
    );

    const updateNodeConfig = React.useCallback(
      (nodeId, config, options = {}) => {
        setNodes((nds) => {
          const nextNodes = nds.map((n) =>
            n.id === nodeId
              ? {
                  ...n,
                  data: {
                    ...(n.data ?? {}),
                    config,
                  },
                }
              : n
          );
          if (options.autoSave) {
            void saveGraph(nextNodes, edges);
          } else {
            setDirty(true);
          }
          return nextNodes;
        });
        if (!options.autoSave) {
          setDirty(true);
        }
      },
      [setNodes, edges, saveGraph]
    );

    const deleteNode = React.useCallback(
      (nodeId) => {
        setNodes((nds) => nds.filter((n) => n.id !== nodeId));
        setEdges((eds) => eds.filter((e) => e.source !== nodeId && e.target !== nodeId));
        setDirty(true);
      },
      [setNodes, setEdges]
    );

    const save = async () => {
      await saveGraph(nodes, edges);
    };

    React.useEffect(() => {
      treeDesignerState.treeId = tree?.id ?? null;
      treeDesignerState.save = save;
      treeDesignerState.isDirty = dirty;
      treeDesignerState.updateNodeConfig = updateNodeConfig;
      treeDesignerState.deleteNode = deleteNode;
      const saveBtn = document.getElementById("tree-designer-save");
      if (saveBtn) {
        saveBtn.disabled = !treeDesignerState.treeId || !dirty;
        saveBtn.textContent = dirty ? "Save changes" : "Saved";
      }
    }, [dirty, tree?.id, updateNodeConfig, deleteNode]);

    return React.createElement(
      "div",
      { className: "tree-designer-inner" },
      React.createElement(
        "div",
        { className: "tree-designer-toolbar" },
        React.createElement("div", { className: "muted" }, tree?.name ?? ""),
        React.createElement("div", { className: "toolbar-actions" })
      ),
      React.createElement(
        "div",
        { className: "tree-designer-body" },
        React.createElement(
          "div",
          { className: "tree-designer-canvas" },
          React.createElement(
            ReactFlow,
            {
              nodes,
              edges,
              onNodesChange: onNodesChangeWithDirty,
              onEdgesChange: onEdgesChangeWithDirty,
              onConnect,
              onDrop,
              onDragOver,
              onNodeDoubleClick,
              onNodeClick,
              onNodeContextMenu,
              nodeTypes: { treeNode: TreeNode },
              defaultEdgeOptions: { type: "step" },
              fitView: true,
              zoomOnDoubleClick: false,
              nodeDragThreshold: 6,
              nodeClickDistance: 6,
            },
            React.createElement(Background, { gap: 16 }),
            React.createElement(Controls),
            React.createElement(MiniMap)
          )
        ),
        React.createElement(
          "div",
          { className: "tree-designer-palette" },
          TREE_ELEMENT_DEFS.filter((item) => !item.debugOnly || appConfig?.debug).map((item) =>
            React.createElement(
              "div",
              {
                key: item.type,
                className: "tree-palette-item",
                draggable: true,
                onDragStart: (event) => {
                  event.dataTransfer.setData("application/reactflow", item.type);
                  event.dataTransfer.effectAllowed = "move";
                },
              },
              item.label
            )
          )
        )
      )
    );
  };

  if (!treeDesignerRoot) {
    treeDesignerRoot = ReactDOM.createRoot(container);
  }
  treeDesignerRoot.render(
    React.createElement(ReactFlowProvider, null, React.createElement(Designer))
  );
}


function parseJsonList(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function bootUi() {
  checkHealth();
  await loadAppConfig();
  try {
    await renderSidebarLists();
  } catch (err) {
    console.warn("Sidebar load failed:", err.message ?? err);
    showLoadError("Sidebar data not available yet.");
  }
  try {
    await renderDashboard();
  } catch (err) {
    console.warn("Dashboard load failed:", err.message ?? err);
    showLoadError("Dashboard data not available yet.");
  }
  startAutoRefresh();
}

bootUi();
