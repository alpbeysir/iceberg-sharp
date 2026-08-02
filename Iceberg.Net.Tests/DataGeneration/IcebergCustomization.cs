using AutoFixture;
using AutoFixture.Kernel;
using AutoFixture.Xunit3;
using System.Data.SqlTypes;

namespace Iceberg.Net.Tests.DataGeneration;

public class AutoIcebergDataAttribute()
    : AutoDataAttribute(() => new Fixture().Customize(new IcebergCustomization()));

public class IcebergCustomization : ICustomization
{
    public void Customize(IFixture fixture)
    {
        // Handle IcebergList<T>
        fixture.Customize(new PostprocessHandler());

        // // Stop AutoFixture from blowing up on deep nesting
        // fixture.Behaviors.OfType<ThrowingRecursionBehavior>().ToList()
        //     .ForEach(b => fixture.Behaviors.Remove(b));
        // fixture.Behaviors.Add(new OmitOnRecursionBehavior());
    }

    private class PostprocessHandler : ISpecimenBuilder, ICustomization
    {
        public void Customize(IFixture fixture)
        {
            fixture.RepeatCount = 4;

            // Add this class to the beginning of the specimen builders list
            fixture.Customizations.Insert(0, this);

            SupportMutableValueTypesCustomization customization = new SupportMutableValueTypesCustomization();
            customization.Customize(fixture);

            fixture.Customize(
                new CompositeCustomization(
                    new DateOnlyFixtureCustomization(),
                    new TimeOnlyFixtureCustomization(),
                    new SqlDecimalFixtureCustomization()
                    // Add other fixture customizations as needed
                ));

            // Handle the recursion depth for your deeply nested Iceberg structures
            fixture.Behaviors.OfType<ThrowingRecursionBehavior>().ToList()
                .ForEach(b => fixture.Behaviors.Remove(b));
            fixture.Behaviors.Add(new OmitOnRecursionBehavior());
        }

        public object Create(object request, ISpecimenContext context)
        {
            return new NoSpecimen();
        }
    }

    public class TimeOnlyFixtureCustomization : ICustomization
    {
        void ICustomization.Customize(IFixture fixture)
        {
            fixture.Customize<TimeOnly>(composer => composer.FromFactory<DateTime>(TimeOnly.FromDateTime));
        }
    }

    public class DateOnlyFixtureCustomization : ICustomization
    {
        void ICustomization.Customize(IFixture fixture)
        {
            fixture.Customize<DateOnly>(composer => composer.FromFactory<DateTime>(DateOnly.FromDateTime));
        }
    }

    public class SqlDecimalFixtureCustomization : ICustomization
    {
        void ICustomization.Customize(IFixture fixture)
        {
            fixture.Customize<SqlDecimal>(composer => composer.FromFactory(
                () => new SqlDecimal(Random.Shared.Next(-9_999_999, 9_999_999) / 100m)));
        }
    }
}
