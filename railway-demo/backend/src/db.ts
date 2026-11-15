import { PrismaClient } from "@prisma/client";

// One shared client. Prisma keeps its own connection pool, so concurrent
// requests can talk to the database at the same time (which is what makes
// the race condition possible in the first place).
const prisma = new PrismaClient();

export type TrainRow = {
  train_id: number;
  train_name: string;
  available_seats: number;
};

export async function listTrains(): Promise<TrainRow[]> {
  const trains = await prisma.train.findMany({ orderBy: { trainId: "asc" } });
  return trains.map((t) => ({
    train_id: t.trainId,
    train_name: t.trainName,
    available_seats: t.availableSeats,
  }));
}

// READ of the shared resource (the seat count for one train).
export async function readSeats(trainId: number): Promise<number | null> {
  const train = await prisma.train.findUnique({
    where: { trainId },
    select: { availableSeats: true },
  });
  return train?.availableSeats ?? null;
}

// WRITE to the shared resource (the seat count for one train).
export async function writeSeats(trainId: number, seats: number): Promise<void> {
  await prisma.train.updateMany({
    where: { trainId },
    data: { availableSeats: seats },
  });
}
