"use client";

import { LogOut, User } from "lucide-react";
import { signOut, useSession } from "next-auth/react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from "@/components/ui/dropdown-menu";

const isDevBypass = process.env.NEXT_PUBLIC_DEV_BYPASS_AUTH === "true";

export default function UserPopover() {
  const { data: session, status } = useSession();
  const user = session?.user;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8 rounded-full bg-dewberry-teal/10 hover:bg-dewberry-teal/20"
          aria-label="Account menu"
        >
          <User className="h-4 w-4 text-dewberry-teal" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-56">
        <DropdownMenuLabel className="font-normal">
          <div className="flex flex-col gap-1">
            <span className="text-sm font-medium">
              {user?.name || (isDevBypass ? "Dev User" : "Signed in")}
            </span>
            <span className="text-xs text-muted-foreground truncate">
              {isDevBypass ? "Dev bypass" : user?.email || "—"}
            </span>
          </div>
        </DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          disabled={isDevBypass || status !== "authenticated"}
          onSelect={() => signOut({ callbackUrl: "/" })}
        >
          <LogOut className="h-4 w-4" />
          <span>Sign out</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
