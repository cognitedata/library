import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { getDefaultPageSize, usePagination } from "./usePagination";

describe("usePagination", () => {
  it("returns default page size and first page", () => {
    const { result } = renderHook(() =>
      usePagination({ items: [1, 2, 3, 4, 5] }),
    );
    expect(result.current.currentPage).toBe(1);
    expect(result.current.pageSize).toBe(getDefaultPageSize());
    expect(result.current.totalItems).toBe(5);
    expect(result.current.paginatedItems).toEqual([1, 2, 3, 4, 5]);
  });

  it("respects initial page and page size", () => {
    const { result } = renderHook(() =>
      usePagination({
        items: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        pageSize: 2,
        initialPage: 2,
      }),
    );
    expect(result.current.currentPage).toBe(2);
    expect(result.current.pageSize).toBe(2);
    expect(result.current.totalPages).toBe(5);
    expect(result.current.paginatedItems).toEqual([3, 4]);
  });

  it("goToPage updates current page and paginatedItems", () => {
    const { result } = renderHook(() =>
      usePagination({
        items: [1, 2, 3, 4, 5],
        pageSize: 2,
      }),
    );
    act(() => {
      result.current.goToPage(2);
    });
    expect(result.current.currentPage).toBe(2);
    expect(result.current.paginatedItems).toEqual([3, 4]);
  });

  it("nextPage and previousPage update page", () => {
    const { result } = renderHook(() =>
      usePagination({
        items: [1, 2, 3, 4, 5],
        pageSize: 2,
      }),
    );
    act(() => {
      result.current.nextPage();
    });
    expect(result.current.currentPage).toBe(2);
    act(() => {
      result.current.previousPage();
    });
    expect(result.current.currentPage).toBe(1);
  });

  it("hasNextPage and hasPreviousPage reflect state", () => {
    const { result } = renderHook(() =>
      usePagination({
        items: [1, 2, 3],
        pageSize: 2,
      }),
    );
    expect(result.current.hasNextPage).toBe(true);
    expect(result.current.hasPreviousPage).toBe(false);
    act(() => result.current.nextPage());
    expect(result.current.hasNextPage).toBe(false);
    expect(result.current.hasPreviousPage).toBe(true);
  });

  it("setPageSize updates page size and resets to page 1", () => {
    const { result } = renderHook(() =>
      usePagination({
        items: [1, 2, 3, 4, 5, 6],
        pageSize: 2,
        initialPage: 2,
      }),
    );
    act(() => {
      result.current.setPageSize(3);
    });
    expect(result.current.pageSize).toBe(3);
    expect(result.current.currentPage).toBe(1);
    expect(result.current.paginatedItems).toEqual([1, 2, 3]);
  });

  it("resetPagination restores initial page and size", () => {
    const { result } = renderHook(() =>
      usePagination({
        items: [1, 2, 3, 4, 5],
        pageSize: 2,
        initialPage: 1,
      }),
    );
    act(() => result.current.goToPage(2));
    act(() => result.current.resetPagination());
    expect(result.current.currentPage).toBe(1);
    expect(result.current.pageSize).toBe(2);
  });

  it("clamps current page when items shrink", () => {
    const { result, rerender } = renderHook(
      ({ items }) => usePagination({ items, pageSize: 2, initialPage: 3 }),
      { initialProps: { items: [1, 2, 3, 4, 5, 6] } },
    );
    expect(result.current.currentPage).toBe(3);
    rerender({ items: [1, 2] });
    expect(result.current.currentPage).toBe(1);
  });
});
