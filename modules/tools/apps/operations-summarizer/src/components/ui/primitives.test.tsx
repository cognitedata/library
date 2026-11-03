import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Card, CardContent, CardHeader, CardTitle } from "./card";
import { Dialog, DialogContent, DialogTitle } from "./dialog";
import { Input } from "./input";
import { Label } from "./label";
import { Textarea } from "./textarea";

describe("ui primitives", () => {
  it("renders card, input, label, textarea, and dialog content", () => {
    render(
      <Card>
        <CardHeader>
          <CardTitle>Title</CardTitle>
        </CardHeader>
        <CardContent>
          <Label htmlFor="name">Name</Label>
          <Input id="name" defaultValue="pump" />
          <Textarea defaultValue="notes" />
        </CardContent>
      </Card>,
    );
    expect(screen.getByText("Title")).toBeInTheDocument();
    expect(screen.getByLabelText("Name")).toHaveValue("pump");

    render(
      <Dialog open>
        <DialogContent>
          <DialogTitle>Dialog title</DialogTitle>
        </DialogContent>
      </Dialog>,
    );
    expect(screen.getByText("Dialog title")).toBeInTheDocument();
  });
});
