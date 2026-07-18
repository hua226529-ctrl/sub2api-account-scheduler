import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const appSource = readFileSync(resolve(process.cwd(), "src/App.vue"), "utf8");

describe("scheduler administrator login contract", () => {
  it("keeps the browser login distinct from the upstream administrator key", () => {
    expect(appSource).toContain("调度中心管理员登录");
    expect(appSource).toContain("v-model=\"adminSecret\"");
    expect(appSource).toContain("placeholder=\"SCHEDULER_ADMIN_SECRET\"");
    expect(appSource).toContain("不要在浏览器中输入 SUB2API_ADMIN_API_KEY");
    expect(appSource).not.toContain("输入 Sub2API 全局管理员密钥");
    expect(appSource).not.toContain("上游管理员密钥登录");
    expect(appSource).not.toContain("v-model=\"apiKey\"");
  });
});
