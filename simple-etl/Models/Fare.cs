namespace simple_etl.Models;

public class Fare
{
    public DateTime TpepPickupDatetime { get; set; }
    public DateTime TpepDropoffDateTime { get; set; }
    public int PassengerCount { get; set; }
    public double TripDistance { get; set; }
    public string StoreAndFwdFlag { get; set; }
    public int PULocationId { get; set; }
    public int DOLocationId { get; set; }
    public double FareAmount { get; set; }
    public double TipAmount { get; set; }

    public override string ToString()
    {
        return
            $"{nameof(TpepPickupDatetime)}: {TpepPickupDatetime}, {nameof(TpepDropoffDateTime)}: {TpepDropoffDateTime}, {nameof(PassengerCount)}: {PassengerCount}, {nameof(TripDistance)}: {TripDistance}, {nameof(StoreAndFwdFlag)}: {StoreAndFwdFlag}, {nameof(PULocationId)}: {PULocationId}, {nameof(DOLocationId)}: {DOLocationId}, {nameof(FareAmount)}: {FareAmount}, {nameof(TipAmount)}: {TipAmount}";
    }
}