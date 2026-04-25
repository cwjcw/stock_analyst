const els = {
  authShell: document.getElementById("authShell"),
  appShell: document.getElementById("appShell"),
  authStatus: document.getElementById("authStatus"),
  appStatus: document.getElementById("appStatus"),
  authTabs: [...document.querySelectorAll("[data-auth-tab]")],
  authForms: {
    login: document.getElementById("loginForm"),
    register: document.getElementById("registerForm"),
    forgot: document.getElementById("forgotForm"),
  },
  loginUserId: document.getElementById("loginUserId"),
  loginPassword: document.getElementById("loginPassword"),
  registerUserId: document.getElementById("registerUserId"),
  registerDisplayName: document.getElementById("registerDisplayName"),
  registerEmail: document.getElementById("registerEmail"),
  registerPassword: document.getElementById("registerPassword"),
  forgotUserId: document.getElementById("forgotUserId"),
  navItems: [...document.querySelectorAll("[data-view]")],
  viewPanels: [...document.querySelectorAll("[data-view-panel]")],
  heroUserName: document.getElementById("heroUserName"),
  heroRiskLevel: document.getElementById("heroRiskLevel"),
  navDisplayName: document.getElementById("navDisplayName"),
  navEmail: document.getElementById("navEmail"),
  logoutBtn: document.getElementById("logoutBtn"),
  tsCode: document.getElementById("tsCode"),
  stockName: document.getElementById("stockName"),
  conceptBoards: document.getElementById("conceptBoards"),
  groupName: document.getElementById("groupName"),
  minutePeriod: document.getElementById("minutePeriod"),
  selectAllBtn: document.getElementById("selectAllBtn"),
  analyzeBtn: document.getElementById("analyzeBtn"),
  addStockBtn: document.getElementById("addStockBtn"),
  removeStockBtn: document.getElementById("removeStockBtn"),
  stockPool: document.getElementById("stockPool"),
  selectedCount: document.getElementById("selectedCount"),
  results: document.getElementById("results"),
  resultCount: document.getElementById("resultCount"),
  recentReports: document.getElementById("recentReports"),
  summaryStockCount: document.getElementById("summaryStockCount"),
  summaryGroupCount: document.getElementById("summaryGroupCount"),
  summaryReportCount: document.getElementById("summaryReportCount"),
  profileUserId: document.getElementById("profileUserId"),
  profileEmail: document.getElementById("profileEmail"),
  profileDisplayName: document.getElementById("profileDisplayName"),
  profilePhone: document.getElementById("profilePhone"),
  profileRiskLevel: document.getElementById("profileRiskLevel"),
  profileBio: document.getElementById("profileBio"),
  profileStrategyNote: document.getElementById("profileStrategyNote"),
  saveProfileBtn: document.getElementById("saveProfileBtn"),
  currentPassword: document.getElementById("currentPassword"),
  newPassword: document.getElementById("newPassword"),
  changePasswordBtn: document.getElementById("changePasswordBtn"),
};

const state = {
  authenticated: false,
  user: null,
  summary: null,
  selectedStocks: new Set(),
  lastResults: [],
};

let lookupTimer = null;

function setAuthStatus(message, isError = false) {
  els.authStatus.textContent = message;
  els.authStatus.classList.toggle("error", isError);
}

function setAppStatus(message, isError = false) {
  els.appStatus.textContent = message;
  els.appStatus.classList.toggle("error", isError);
}

async function api(url, method = "GET", body = null) {
  const options = { method, headers: { "Content-Type": "application/json" } };
  if (body) options.body = JSON.stringify(body);
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.error || `请求失败，状态码 ${response.status}`);
  }
  return payload.data;
}

function switchAuthTab(name) {
  els.authTabs.forEach((tab) => tab.classList.toggle("active", tab.dataset.authTab === name));
  Object.entries(els.authForms).forEach(([key, form]) => form.classList.toggle("hidden", key !== name));
}

function switchView(name) {
  els.navItems.forEach((item) => item.classList.toggle("active", item.dataset.view === name));
  els.viewPanels.forEach((panel) => panel.classList.toggle("active", panel.dataset.viewPanel === name));
}

function showAuthShell() {
  els.authShell.classList.remove("hidden");
  els.appShell.classList.add("hidden");
}

function showAppShell() {
  els.authShell.classList.add("hidden");
  els.appShell.classList.remove("hidden");
}

function groupedStocks() {
  const groups = new Map();
  (state.user?.stocks || []).forEach((stock) => {
    const key = stock.group_name || "默认分组";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(stock);
  });
  return [...groups.entries()];
}

function syncUserView() {
  const user = state.user;
  if (!user) return;
  els.heroUserName.textContent = user.display_name || user.user_id;
  els.heroRiskLevel.textContent = user.risk_level || "稳健";
  els.navDisplayName.textContent = user.display_name || user.user_id;
  els.navEmail.textContent = user.email || "未设置邮箱";

  els.profileUserId.value = user.user_id || "";
  els.profileEmail.value = user.email || "";
  els.profileDisplayName.value = user.display_name || "";
  els.profilePhone.value = user.phone || "";
  els.profileRiskLevel.value = user.risk_level || "稳健";
  els.profileBio.value = user.bio || "";
  els.profileStrategyNote.value = user.strategy_note || "";
}

function renderSummary() {
  const summary = state.summary || { stock_count: 0, group_count: 0, report_count: 0, latest_reports: [] };
  els.summaryStockCount.textContent = summary.stock_count ?? 0;
  els.summaryGroupCount.textContent = summary.group_count ?? 0;
  els.summaryReportCount.textContent = summary.report_count ?? 0;

  if (!summary.latest_reports?.length) {
    els.recentReports.innerHTML = '<div class="empty">还没有最近报告，先去分析几只股票。</div>';
    return;
  }

  els.recentReports.innerHTML = summary.latest_reports
    .map(
      (item) => `
        <article class="report-item">
          <div>
            <strong>${item.ts_code}</strong>
            <p>${item.mtime}</p>
          </div>
          <span>${item.size_kb} KB</span>
        </article>
      `
    )
    .join("");
}

function renderStockPool() {
  const groups = groupedStocks();
  if (!groups.length) {
    els.stockPool.innerHTML = '<div class="empty">当前还没有股票，先添加你的关注标的。</div>';
    els.selectedCount.textContent = "0 已选";
    return;
  }

  els.stockPool.innerHTML = groups
    .map(([groupName, stocks]) => {
      const items = stocks
        .map((stock) => {
          const code = stock.ts_code;
          const checked = state.selectedStocks.has(code) ? "checked" : "";
          return `
            <label class="stock-item">
              <input type="checkbox" data-code="${code}" ${checked} />
              <span class="stock-meta">
                <strong>${code}</strong>
                <em>${stock.stock_name || "未命名股票"}</em>
              </span>
            </label>
          `;
        })
        .join("");
      return `
        <section class="stock-group">
          <div class="group-title">${groupName}</div>
          <div class="group-list">${items}</div>
        </section>
      `;
    })
    .join("");

  els.selectedCount.textContent = `${state.selectedStocks.size} 已选`;
  els.stockPool.querySelectorAll("input[type='checkbox']").forEach((checkbox) => {
    checkbox.addEventListener("change", () => {
      const code = checkbox.dataset.code;
      if (checkbox.checked) state.selectedStocks.add(code);
      else state.selectedStocks.delete(code);
      els.selectedCount.textContent = `${state.selectedStocks.size} 已选`;
    });
  });
}

function downloadMarkdownFile(tsCode, markdown) {
  const blob = new Blob([markdown], { type: "text/markdown;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${tsCode.replace(".", "_")}_分析报告.md`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function renderResults(results = []) {
  state.lastResults = results;
  els.resultCount.textContent = `${results.length} 结果`;
  if (!results.length) {
    els.results.innerHTML = '<div class="empty">分析结果会显示在这里，支持直接展开查看或导出 Markdown。</div>';
    return;
  }

  els.results.innerHTML = results
    .map(
      (item, index) => `
        <article class="result-card">
          <header class="result-head">
            <div>
              <h4>${item.ts_code}</h4>
              <p>${item.report_path}</p>
            </div>
            <div class="decision-stack">
              <span class="decision final">综合：${item.final_signal || "-"}</span>
              <span class="decision trend">趋势：${item.trend_decision}</span>
              <span class="decision timing">时机：${item.timing_decision}</span>
            </div>
          </header>
          <div class="result-actions">
            <button class="btn export-btn" data-result-index="${index}">导出 Markdown</button>
          </div>
          <details>
            <summary>展开完整报告</summary>
            <div class="markdown">${marked.parse(item.markdown || "")}</div>
          </details>
        </article>
      `
    )
    .join("");

  els.results.querySelectorAll(".export-btn").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.lastResults[Number(button.dataset.resultIndex)];
      if (!item) return;
      downloadMarkdownFile(item.ts_code, item.markdown || "");
    });
  });
}

function hydrateUser(data) {
  state.authenticated = Boolean(data?.authenticated);
  state.user = data?.user || null;
  state.summary = data?.summary || null;
  if (!state.authenticated || !state.user) {
    showAuthShell();
    return;
  }
  showAppShell();
  syncUserView();
  renderSummary();
  renderStockPool();
  renderResults([]);
  switchView("overview");
}

async function refreshMe() {
  const data = await api("/api/me");
  hydrateUser(data);
}

async function refreshDashboard() {
  const data = await api("/api/dashboard");
  state.summary = data.summary;
  renderSummary();
}

async function refreshStocks() {
  const data = await api("/api/stocks");
  state.user.stocks = data.stocks || [];
  renderStockPool();
  await refreshDashboard();
}

async function lookupStockByCode(rawCode) {
  const trimmed = (rawCode || "").trim().toUpperCase();
  if (!/^\d{6}$/.test(trimmed)) {
    return;
  }
  setAppStatus(`正在匹配股票 ${trimmed} ...`);
  const data = await api("/api/stocks/lookup", "POST", { code: trimmed });
  els.tsCode.value = data.ts_code || trimmed;
  if (!els.stockName.value.trim()) {
    els.stockName.value = data.stock_name || "";
  } else {
    els.stockName.value = data.stock_name || els.stockName.value.trim();
  }
  els.conceptBoards.value = (data.concept_boards || []).join("、");
  setAppStatus(data.warning || `已匹配 ${data.stock_name || trimmed}`);
}

els.authTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    switchAuthTab(tab.dataset.authTab);
    setAuthStatus("请按照当前页面提示完成操作。");
  });
});

els.authForms.login.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    setAuthStatus("正在登录，请稍候...");
    const data = await api("/api/auth/login", "POST", {
      user_id: els.loginUserId.value.trim(),
      password: els.loginPassword.value.trim(),
    });
    state.user = data.user;
    state.authenticated = true;
    setAuthStatus("登录成功。");
    await refreshMe();
  } catch (error) {
    setAuthStatus(error.message, true);
  }
});

els.authForms.register.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    setAuthStatus("正在注册，请稍候...");
    const data = await api("/api/auth/register", "POST", {
      user_id: els.registerUserId.value.trim(),
      display_name: els.registerDisplayName.value.trim(),
      email: els.registerEmail.value.trim(),
      password: els.registerPassword.value.trim(),
    });
    state.user = data.user;
    state.authenticated = true;
    setAuthStatus(data.warning || "注册成功，欢迎邮件已发送。");
    await refreshMe();
  } catch (error) {
    setAuthStatus(error.message, true);
  }
});

els.authForms.forgot.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    setAuthStatus("正在发送新密码，请稍候...");
    const data = await api("/api/auth/forgot-password", "POST", {
      user_id: els.forgotUserId.value.trim(),
    });
    setAuthStatus(data.message || "新的随机密码已发送到注册邮箱。");
  } catch (error) {
    setAuthStatus(error.message, true);
  }
});

els.navItems.forEach((item) => {
  item.addEventListener("click", () => switchView(item.dataset.view));
});

els.logoutBtn.addEventListener("click", async () => {
  try {
    await api("/api/auth/logout", "POST");
    state.authenticated = false;
    state.user = null;
    state.summary = null;
    state.selectedStocks.clear();
    showAuthShell();
    switchAuthTab("login");
    setAuthStatus("已退出登录。");
  } catch (error) {
    setAppStatus(error.message, true);
  }
});

els.saveProfileBtn.addEventListener("click", async () => {
  try {
    setAppStatus("正在保存用户资料...");
    const data = await api("/api/me/profile", "POST", {
      display_name: els.profileDisplayName.value.trim(),
      phone: els.profilePhone.value.trim(),
      risk_level: els.profileRiskLevel.value,
      bio: els.profileBio.value.trim(),
      strategy_note: els.profileStrategyNote.value.trim(),
    });
    state.user = { ...state.user, ...data.user };
    syncUserView();
    setAppStatus("用户资料已保存。");
  } catch (error) {
    setAppStatus(error.message, true);
  }
});

els.tsCode.addEventListener("input", () => {
  const rawCode = els.tsCode.value.trim().toUpperCase();
  if (lookupTimer) clearTimeout(lookupTimer);
  if (rawCode.length < 6) {
    els.conceptBoards.value = "";
    return;
  }
  lookupTimer = setTimeout(async () => {
    try {
      await lookupStockByCode(rawCode);
    } catch (error) {
      els.conceptBoards.value = "";
      setAppStatus(error.message, true);
    }
  }, 350);
});

els.changePasswordBtn.addEventListener("click", async () => {
  try {
    setAppStatus("正在修改密码...");
    const data = await api("/api/me/password", "POST", {
      current_password: els.currentPassword.value.trim(),
      new_password: els.newPassword.value.trim(),
    });
    els.currentPassword.value = "";
    els.newPassword.value = "";
    setAppStatus(data.message || "密码修改成功。");
  } catch (error) {
    setAppStatus(error.message, true);
  }
});

els.addStockBtn.addEventListener("click", async () => {
  try {
    const ts_code = els.tsCode.value.trim().toUpperCase();
    if (!ts_code) {
      setAppStatus("请输入股票代码。", true);
      return;
    }
    setAppStatus(`正在加入股票 ${ts_code}...`);
    const data = await api("/api/stocks/add", "POST", {
      ts_code,
      stock_name: els.stockName.value.trim(),
      group_name: els.groupName.value.trim() || "默认分组",
    });
    state.user.stocks = data.stocks || [];
    els.tsCode.value = "";
    els.stockName.value = "";
    els.conceptBoards.value = "";
    renderStockPool();
    await refreshDashboard();
    setAppStatus(`已加入股票：${ts_code}`);
  } catch (error) {
    setAppStatus(error.message, true);
  }
});

els.removeStockBtn.addEventListener("click", async () => {
  try {
    const ts_code = els.tsCode.value.trim().toUpperCase();
    if (!ts_code) {
      setAppStatus("请输入要移除的股票代码。", true);
      return;
    }
    setAppStatus(`正在移除股票 ${ts_code}...`);
    const data = await api("/api/stocks/remove", "POST", {
      ts_code,
      group_name: els.groupName.value.trim(),
    });
    state.user.stocks = data.stocks || [];
    state.selectedStocks.delete(ts_code);
    els.conceptBoards.value = "";
    renderStockPool();
    await refreshDashboard();
    setAppStatus(`已移除股票：${ts_code}`);
  } catch (error) {
    setAppStatus(error.message, true);
  }
});

els.selectAllBtn.addEventListener("click", () => {
  const codes = (state.user?.stocks || []).map((stock) => stock.ts_code);
  const allSelected = codes.length > 0 && codes.every((code) => state.selectedStocks.has(code));
  if (allSelected) state.selectedStocks.clear();
  else codes.forEach((code) => state.selectedStocks.add(code));
  renderStockPool();
});

els.analyzeBtn.addEventListener("click", async () => {
  try {
    const ts_codes = [...state.selectedStocks];
    if (!ts_codes.length) {
      setAppStatus("请先勾选至少一只股票。", true);
      return;
    }
    els.analyzeBtn.disabled = true;
    setAppStatus(`正在分析 ${ts_codes.length} 只股票，请稍候...`);
    const data = await api("/api/analyze", "POST", {
      ts_codes,
      minute_period: els.minutePeriod.value,
    });
    renderResults(data.results || []);
    switchView("results");
    await refreshDashboard();
    setAppStatus(`分析完成，共返回 ${data.count} 份结果。`);
  } catch (error) {
    setAppStatus(error.message, true);
  } finally {
    els.analyzeBtn.disabled = false;
  }
});

refreshMe()
  .then(() => {
    if (!state.authenticated) {
      switchAuthTab("login");
      showAuthShell();
    }
  })
  .catch((error) => {
    showAuthShell();
    setAuthStatus(error.message, true);
  });
