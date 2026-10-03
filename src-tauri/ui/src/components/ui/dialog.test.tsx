// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";

afterEach(cleanup);
import { useState } from "react";
import { Input } from "./input";
import "@testing-library/jest-dom/vitest";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "./dialog";

function Harness() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        Open settings
      </button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogTitle>Settings</DialogTitle>
          <DialogDescription>Apply changes immediately.</DialogDescription>
          <Input data-autofocus aria-label="Listen port" />
        </DialogContent>
      </Dialog>
    </>
  );
}

describe("Dialog", () => {
  it("focuses the first control when opened non-modally", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByText("Open settings"));
    const dialog = await screen.findByRole("dialog");
    expect(dialog).toBeInTheDocument();
    expect(document.activeElement?.tagName).toBe("INPUT");
    expect(document.activeElement).toHaveAttribute("aria-label", "Listen port");
  });

  it("closes on Escape and returns focus to the opener", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByText("Open settings"));
    await screen.findByRole("dialog");
    await user.keyboard("{Escape}");
    await user.tab();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(document.activeElement?.tagName).toBe("BUTTON");
    expect(document.activeElement?.textContent).toBe("Open settings");
  });
});
